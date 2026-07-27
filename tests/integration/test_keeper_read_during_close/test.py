"""Test that read requests from session B are not silently dropped
when the write request they were batched with belongs to session A
and session A expires before the batch commits.

The bug: read_request_queue keys reads by (sessionA.id, sessionA.xid).
When sessionA expires, finishSession erases read_request_queue[sessionA.id],
silently dropping sessionB's read. SessionB's get() then hangs until timeout.
"""

import logging
import queue
import socket
import struct
import threading
import time
import traceback
import os

import pytest
from kazoo.client import KazooClient

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)


class MicroClient:
    @staticmethod
    def make_connect_request(timeout_ms=10000):
        """ConnectRequest: protocol_version(4) + last_zxid(8) + timeout(4) + session_id(8) + passwd_len(4) + passwd(16) + readonly(1)"""
        return struct.pack('>iqiqi 16s',
            0,            # protocol version
            0,            # last zxid seen
            timeout_ms,   # requested timeout
            0,            # session id (0 = new session)
            16,           # passwd length
            b'\x00' * 16, # passwd
        )

    @staticmethod
    def make_set_request(xid, path: bytes, data: bytes, version=-1):
        body = struct.pack('>ii', xid, 5)  # xid + opcode 5 = SetData
        body += struct.pack('>i', len(path)) + path.encode()
        body += struct.pack('>i', len(data)) + data
        body += struct.pack('>i', version)
        return body

    @staticmethod
    def make_frame(payload: bytes) -> bytes:
        return struct.pack('>i', len(payload)) + payload

    @staticmethod
    def recv_exactly(sock, n):
        """Receive exactly n bytes or raise."""
        buf = b''
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError(f"Connection closed after {len(buf)}/{n} bytes")
            buf += chunk
        return buf

    @staticmethod
    def recv_frame(sock):
        """Read a length-prefixed ZK frame."""
        length = struct.unpack('>i', MicroClient.recv_exactly(sock, 4))[0]
        return MicroClient.recv_exactly(sock, length)

    @staticmethod
    def parse_connect_response(data):
        # ConnectResponse: protocol_version(4) + timeout(4) + session_id(8) + passwd_len(4) + passwd(variable)
        proto_ver, timeout, session_id = struct.unpack_from('>iiq', data, 0)
        passwd_len = struct.unpack_from('>i', data, 16)[0]
        passwd = data[20:20 + passwd_len]
        return {
            'protocol_version': proto_ver,
            'timeout': timeout,
            'session_id': hex(session_id),
            'passwd': passwd.hex(),
        }

    @staticmethod
    def parse_reply_header(data):
        # ReplyHeader: xid(4) + zxid(8) + err(4)
        xid, zxid, err = struct.unpack_from('>iqi', data, 0)
        return {
            'xid': xid,
            'zxid': zxid,
            'err': err,
        }

    @staticmethod
    def parse_stat(data, offset=0):
        """Parse a ZK Stat structure (10 fields, 8+8+8+8+4+4+4+8+4+4+8 = 68 bytes)."""
        fields = struct.unpack_from('>qqqqiiiqiiq', data, offset)
        names = ['czxid', 'mzxid', 'ctime', 'mtime', 'version', 'cversion',
                'aversion', 'ephemeralOwner', 'dataLength', 'numChildren', 'pzxid']
        return dict(zip(names, fields))

    def __init__(self, hostname, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5.0)
        self.sock.connect((hostname, port))
        self.next_xid = 1;

        self.sock.sendall(MicroClient.make_frame(MicroClient.make_connect_request()))

        connect_resp = MicroClient.recv_frame(self.sock)
        parsed = MicroClient.parse_connect_response(connect_resp)

        if int(parsed['session_id'], 16) == 0:
            raise Exception("Server rejected session (session_id=0)")

    def send_set_request(self, path, data, version=-1):
        xid = self.next_xid
        self.next_xid += 1
        self.sock.sendall(MicroClient.make_frame(MicroClient.make_set_request(xid, path, data, version)))
        return xid

    def recv_set_response(self):
        set_resp = MicroClient.recv_frame(self.sock)
        header = MicroClient.parse_reply_header(set_resp)
        return header


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_zk(timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, "node", timeout=timeout)


def kill_session_socket(zk):
    """Forcefully kill the underlying TCP connection without sending Close.
    This causes the session to expire after session_timeout_ms."""
    zk._connection._socket.close()

def test_read_not_dropped_on_session_close(started_cluster):
    """Verify that reads from one session are not silently lost
    when a concurrent writer session dies."""

    reader_zk = get_zk()
    reader_zk.create("/test_read_close", b"initial")

    # Sanity-check our custom keeper client implementation.
    hostname = cluster.get_instance_ip("node")
    port = 9181
    temp_zk = MicroClient(hostname, port)
    xid = temp_zk.send_set_request("/test_read_close", b"micro")
    resp = temp_zk.recv_set_response()
    assert resp["xid"] == xid
    assert resp["err"] == 0

    assert reader_zk.get("/test_read_close")[0] == b"micro"

    logging.getLogger('kazoo').setLevel(logging.WARNING)

    fail_event = threading.Event()
    stop_event = threading.Event()
    errors = queue.Queue()

    def reader_loop():
        """Continuously read /test_read_close. Each get() should complete
        within a reasonable time. If it hangs, that's the bug."""
        counter = 0
        try:
            zk = get_zk(timeout=10.0)
            while not stop_event.is_set():
                zk.get("/test_read_close")
                counter += 1
        except:
            errors.put(traceback.format_exc())
            fail_event.set()
        # This prints around 2200 (on my machine, as of the time of writing, with 10 second test duration).
        print(f"sent {counter} read requests")

    def writer_loop():
        """Rapidly create sessions that write and then die (raw socket close).
        The intent is that the write and a concurrent read from reader_loop
        end up in the same batch, keyed under this writer session's identity."""
        counter = 0
        try:
            while not stop_event.is_set():
                counter += 1
                writer_zk = MicroClient(hostname, port)
                writer_zk.send_set_request("/test_read_close", f"data_{counter}".encode())
                # Kill the TCP socket without sending Close.
                # The session will expire after session_timeout_ms (3s).
                writer_zk.sock.close()

                # assert writer_zk.recv_set_response()["err"] == 0
        except:
            errors.put(traceback.format_exc())
            fail_event.set()
        # This prints around 1800 (on my machine, as of the time of writing, with 10 second test duration).
        print(f"sent {counter} write requests")


    # Run the reader and writer concurrently
    reader_thread = threading.Thread(target=reader_loop, daemon=True)
    writer_thread = threading.Thread(target=writer_loop, daemon=True)

    reader_thread.start()
    writer_thread.start()

    # Run for a few seconds — enough for many session create/expire cycles
    # given session_timeout_ms=3000, dead_session_check_period_ms=500.
    fail_event.wait(10)

    stop_event.set()
    reader_thread.join(timeout=10)
    writer_thread.join(timeout=10)

    if fail_event.is_set():
        raise Exception(errors.get(block=False))

    assert not reader_thread.is_alive(), "Reader thread is stuck"
    assert not writer_thread.is_alive(), "Writer thread is stuck"

    # Cleanup
    reader_zk.delete("/test_read_close")
    reader_zk.stop()
    reader_zk.close()
