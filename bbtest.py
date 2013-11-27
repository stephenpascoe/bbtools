#!/usr/bin/env python

import sys
import subprocess as S
import re
import datetime as DT

DEFAULT_TEST_TIME = 10
PROGRESS_REXP = re.compile(r'bbcp: '
                           r'(?P<pid>\d+)\s+(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)'
                           r'\s+(?P<pcdone>\d+)% done; (?P<bps>[0-9.]+) (?P<unit>.*)/s')

class error(Exception):
    pass

class BBNode(object):
    """

    :ivar hostname: The hostname or IP of the node.  None indicates local node.
    :ivar listen_ports: A tuple of ints (min_port, max_port) indicating 
        the port range at which this node can listen.  None indicates any port.
        False indicates no ports.
    :ivar connect_ports: A tuple of ints (min_port, max_port) indicating
        the port range at which this node cann connect to listening nodes.
        None indicates any port.  False indicates no ports.

    """
    def __init__(self, hostname, listen_ports=None, connect_ports=None):
        self.hostname = hostname
        self.listen_ports = listen_ports
        self.connect_ports = connect_ports

    def pathspec(self, path, username=None):
        if self.hostname is None:
            return path
        if username:
            idstr = '%s@' % username
        else:
            idstr = ''
        return '%s%s:%s' % (idstr, self.hostname, path)

    #!TODO: configurable path to bbcp executable


def match_ports(src_node, snk_node):
    """
    Deduce the supported port range and protocol direction for a pair
    of nodes.

    :return: ((min_port, max_port), is_reverse_protcol) 
        where is_reverse_protocol is a boolean indicating whether to include
        the "-z" flag.

    """

    def _match_ports(ports1, ports2):
        if ports1 is False or ports2 is False:
            return False
        if ports1 is None:
            return ports2
        elif ports2 is None:
            return ports1

        min_p1, max_p1 = ports1
        min_p2, max_p2 = ports2

        min_port = max((min_p1, min_p2))
        max_port = min((max_p1, max_p2))

        return (min_port, max_port)

    # Deduce the possible port range for each direction
    default_proto_ports = _match_ports(src_node.connect_ports,
                                       snk_node.listen_ports)
    reverse_proto_ports = _match_ports(src_node.listen_ports,
                                       snk_node.connect_ports)
    
    if default_proto_ports == False:
        return (reverse_proto_ports, True)
    else:
        return (default_proto_ports, False)


import unittest
class TestPortMatch(unittest.TestSuite):
    def setUp(self):
        # typical server node
        self.node1 = BBNode('myhost1', (50000, 50100), None)
        # typical client node
        self.node2 = BBNode('myhost2', False, None)
        # typical client node behind restricted firewall
        self.node3 = BBNode('myhost3', False, (50000, 50100))
        # client node blocking all ports
        self.node4 = BBNode('myhost4', False, False)
        # client node with overlapping port range
        self.node5 = BBNode('myhost5', False, (40000, 50050)) 

    def test1(self):
        port_range, is_reverse_proto = match_ports(self.node1, self.node2)

        assert is_reverse_proto
        assert port_range == (50000, 50100)

    def test2(self):
        port_range, is_reverse_proto = match_ports(self.node2, self.node1)

        assert not is_reverse_proto
        assert port_range == (50000, 50100)

    def test3(self):
        port_range, is_reverse_proto = match_ports(self.node1, self.node3)

        assert is_reverse_proto
        assert port_range == (50000, 50100)

    def test4(self):
        port_range, is_reverse_proto = match_ports(self.node3, self.node1)

        assert not is_reverse_proto
        assert port_range == (50000, 50100)

    def test5(self):
        port_range, is_reverse_proto = match_ports(self.node1, self.node4)

        assert port_range == False

    def test6(self):
        port_range, is_reverse_proto = match_ports(self.node4, self.node1)

        assert port_range == False
        
    def test7(self):
        port_range, is_reverse_proto = match_ports(self.node1, self.node5)

        assert is_reverse_proto
        assert port_range == (50000, 50050)

    def test8(self):
        port_range, is_reverse_proto = match_ports(self.node5, self.node1)

        assert not is_reverse_proto
        assert port_range == (50000, 50050)


def network_test(host1, host2, timeout=DEFAULT_TEST_TIME, streams=1):
    port_range, is_reverse_proto = match_ports(host1, host2)
    
    args = ['-P', '2', '-t', str(timeout), '-s', str(streams)]
    if port_range:
        args += ['--port', '%d:%d' % port_range]
    if is_reverse_proto:
        args += ['-z']

    srcspec = host1.pathspec('/dev/zero')
    snkspec = host2.pathspec('/dev/null')

    cmd = ['bbcp'] + args + [srcspec, snkspec]
    print ' '.join(cmd)

    p = S.Popen(cmd, stdin=None, stdout=S.PIPE, stderr=S.STDOUT)
    for dt, mbps in parse_progress(p.stdout):
        print '%s %s' % (dt, mbps)

def parse_progress(progress_fh):
    #!TODO: parse stream of progress lines
    now = DT.datetime.now()
    for line in progress_fh:
        m = PROGRESS_REXP.match(line)
        if m:
            d = m.groupdict()
            dt = DT.datetime(now.year, now.month, now.day,
                             int(d['hour']),
                             int(d['minute']),
                             int(d['second']),
                             )
            # Normalise units to MB
            
            if d['unit'] == 'MB':
                mbps = float(d['bps'])
            elif d['unit'] == 'KB':
                mbps = float(d['bps']) * 2**10
            elif d['unit'] == 'GB':
                mbps = float(d['bps']) / 2**10
            else:
                raise Exception('Unknown bandwith unit %s' % d['unit'])

            yield (dt, mbps)

def bdp(bandwidth, delay):
    return float(bandwidth) / 8 * delay

def main(argv=sys.argv):

    target, = argv[1:]

    host1 = BBNode(None, listen_ports=False)
    host2 = BBNode(target, listen_ports=(50000, 50100))

    print '=========== Network Only Tests =============='
    for streams in [1, 4, 8, 16, 32]:
        print '=== local --> target; streams = %d' % streams
        network_test(host1, host2, streams=streams)
    for streams in [1, 4, 8, 16, 32]:
        print '=== local <-- target; streams = %d' % streams
        network_test(host2, host1, streams=streams)
    print '============================================='



if __name__ == '__main__':
    main()
