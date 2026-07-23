#!/usr/bin/env python3
# Tags: no-fasttest
"""Test native TCP protocol input validation.

Verifies that the server properly validates wire-supplied values:
1. QueryProcessingStage::Enum — reject invalid values (UBSan fix)
2. Protocol::Compression — reject invalid values
3. query_kind — reject invalid values (NO_QUERY, unknown)
"""

import os
import random
import socket
import struct
import sys
import time

CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT_TCP", 9000))
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")


# -- Minimal native protocol implementation ----------------------------------

def write_varuint(value):
    result = bytearray()
    while value > 0x7F:
        result.append(0x80 | (value & 0x7F))
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def write_string(s):
    if isinstance(s, str):
        s = s.encode("utf-8")
    return write_varuint(len(s)) + s


def write_uint8(value):
    return struct.pack("<B", value)


def write_uint64(value):
    return struct.pack("<Q", value)


def read_varuint(sock):
    result = 0
    shift = 0
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("Connection closed")
        byte = b[0]
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed")
        data += chunk
    return data


def read_string(sock):
    length = read_varuint(sock)
    data = recv_exact(sock, length) if length else b""
    return data.decode("utf-8")


# Protocol constants
CLIENT_HELLO = 0
CLIENT_QUERY = 1
CLIENT_DATA = 2
SERVER_HELLO = 0
SERVER_EXCEPTION = 2
SERVER_PROGRESS = 3
SERVER_END_OF_STREAM = 5
SERVER_PROFILE_INFO = 6
SERVER_PROFILE_EVENTS = 14

# Use 54449: includes initial_query_start_time + interserver secret field
CLIENT_REVISION = 54449

QUERY_KIND_INITIAL = 1
INTERFACE_TCP = 1


def send_hello(sock, user="default", password=""):
    pkt = bytearray()
    pkt += write_varuint(CLIENT_HELLO)
    pkt += write_string("ClickHouse test")
    pkt += write_varuint(25)
    pkt += write_varuint(1)
    pkt += write_varuint(CLIENT_REVISION)
    pkt += write_string("")        # default database
    pkt += write_string(user)
    pkt += write_string(password)
    sock.sendall(pkt)


def recv_hello(sock):
    pkt_type = read_varuint(sock)
    if pkt_type == SERVER_EXCEPTION:
        code = struct.unpack("<I", recv_exact(sock, 4))[0]
        name = read_string(sock)
        message = read_string(sock)
        raise Exception(f"Server exception {code}: {name}: {message}")
    assert pkt_type == SERVER_HELLO, f"Expected Hello, got {pkt_type}"
    _name = read_string(sock)
    _major = read_varuint(sock)
    _minor = read_varuint(sock)
    _rev = read_varuint(sock)
    if CLIENT_REVISION >= 54058:
        _tz = read_string(sock)
    if CLIENT_REVISION >= 54372:
        _display = read_string(sock)
    if CLIENT_REVISION >= 54401:
        _patch = read_varuint(sock)


def build_client_info(query_kind=QUERY_KIND_INITIAL, user="default"):
    buf = bytearray()
    buf += write_uint8(query_kind)
    buf += write_string(user)               # initial_user
    buf += write_string("")                 # initial_query_id
    buf += write_string("127.0.0.1:9000")   # initial_address
    buf += write_uint64(int(time.time() * 1e6))  # initial_query_start_time_us
    buf += write_uint8(INTERFACE_TCP)
    buf += write_string("testuser")         # os_user
    buf += write_string("testhost")         # client_hostname
    buf += write_string("ClickHouse test")  # client_name
    buf += write_varuint(25)                # version_major
    buf += write_varuint(1)                 # version_minor
    buf += write_varuint(CLIENT_REVISION)   # client_tcp_protocol_version
    buf += write_string("")                 # quota_key
    buf += write_varuint(0)                 # distributed_depth
    buf += write_varuint(1)                 # version_patch
    buf += write_uint8(0)                   # no OpenTelemetry
    return bytes(buf)


def build_settings(settings_dict=None):
    buf = bytearray()
    if settings_dict:
        for name, value in settings_dict.items():
            buf += write_string(name)
            buf += write_varuint(0x1)  # flags: IMPORTANT
            buf += write_string(str(value))
    buf += write_string("")  # terminator
    return bytes(buf)


def send_query(sock, query_text, stage=2, compression=0,
               query_kind=QUERY_KIND_INITIAL, settings=None, user="default"):
    pkt = bytearray()
    pkt += write_varuint(CLIENT_QUERY)
    pkt += write_string(f"test-{random.randint(0, 2**63)}")
    pkt += build_client_info(query_kind=query_kind, user=user)
    pkt += build_settings(settings)
    pkt += write_string("")  # interserver secret hash (empty)
    pkt += write_varuint(stage)
    pkt += write_varuint(compression)
    pkt += write_string(query_text)
    sock.sendall(pkt)


def send_empty_block(sock):
    pkt = bytearray()
    pkt += write_varuint(CLIENT_DATA)
    pkt += write_string("")     # temp table name
    pkt += write_varuint(0)     # block info end marker
    pkt += write_varuint(0)     # columns
    pkt += write_varuint(0)     # rows
    sock.sendall(pkt)


def skip_block(sock):
    _temp = read_string(sock)
    while True:
        fn = read_varuint(sock)
        if fn == 0:
            break
        if fn == 1:
            recv_exact(sock, 1)
        elif fn == 2:
            recv_exact(sock, 4)
    nc = read_varuint(sock)
    nr = read_varuint(sock)
    for _ in range(nc):
        _cn = read_string(sock)
        ct = read_string(sock)
        if nr == 0:
            continue
        if "Int8" in ct or "UInt8" in ct or "Enum8" in ct:
            recv_exact(sock, nr)
        elif "Int16" in ct or "UInt16" in ct:
            recv_exact(sock, nr * 2)
        elif "Int32" in ct or "UInt32" in ct or "Float32" in ct:
            recv_exact(sock, nr * 4)
        elif "Int64" in ct or "UInt64" in ct or "Float64" in ct or "DateTime64" in ct:
            recv_exact(sock, nr * 8)
        elif "String" in ct:
            for _ in range(nr):
                read_string(sock)
        else:
            recv_exact(sock, nr * 8)


def get_response(sock, timeout=5.0):
    """Returns (ok, message). ok=True means query succeeded."""
    sock.settimeout(timeout)
    try:
        while True:
            pkt_type = read_varuint(sock)
            if pkt_type == SERVER_EXCEPTION:
                code = struct.unpack("<I", recv_exact(sock, 4))[0]
                name = read_string(sock)
                message = read_string(sock)
                _stack = read_string(sock)
                _nested = recv_exact(sock, 1)
                return False, f"{code}:{message}"
            elif pkt_type == SERVER_END_OF_STREAM:
                return True, "OK"
            elif pkt_type in (1, 14):  # Data or ProfileEvents
                skip_block(sock)
            elif pkt_type == SERVER_PROGRESS:
                for _ in range(3):
                    read_varuint(sock)
                if CLIENT_REVISION >= 54372:
                    read_varuint(sock)
                    read_varuint(sock)
                if CLIENT_REVISION >= 54448:
                    read_varuint(sock)
            elif pkt_type == SERVER_PROFILE_INFO:
                for _ in range(7):
                    read_varuint(sock)
                recv_exact(sock, 3)
            else:
                return True, f"pkt_type={pkt_type}"
    except socket.timeout:
        return False, "timeout"
    except Exception as e:
        return False, str(e)


def run_query(host, port, query="SELECT 1", stage=2, compression=0,
              query_kind=QUERY_KIND_INITIAL, settings=None,
              user="default", password=""):
    """Connect, send query, return (ok, message)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((host, port))
        send_hello(sock, user=user, password=password)
        recv_hello(sock)
        send_query(sock, query, stage=stage, compression=compression,
                   query_kind=query_kind, settings=settings, user=user)
        send_empty_block(sock)
        return get_response(sock)
    finally:
        sock.close()


# -- Tests -------------------------------------------------------------------

def test_valid_queries_still_work(host, port):
    """Sanity check: normal queries still succeed."""
    ok, msg = run_query(host, port, query="SELECT 1")
    assert ok, f"SELECT 1 should succeed, got: {msg}"
    ok, msg = run_query(host, port, query="SELECT 1",
                        query_kind=QUERY_KIND_INITIAL)
    assert ok, f"INITIAL_QUERY SELECT 1 should succeed, got: {msg}"
    print("valid queries work")


def test_invalid_stage(host, port):
    """Invalid QueryProcessingStage values must be rejected."""
    for stage in [5, 6, 120, 255]:
        ok, msg = run_query(host, port, stage=stage)
        assert not ok, f"stage={stage} should be rejected, got ok"
        assert "Unknown query processing stage" in msg, f"stage={stage}: {msg}"
    # Stage 7 (QueryPlan) is valid but disabled by default
    ok, msg = run_query(host, port, stage=7)
    assert not ok, f"stage=7 should fail (disabled), got ok"
    print("invalid stage values are rejected")


def test_invalid_query_kind(host, port):
    """Invalid query_kind values must be rejected.
    NO_QUERY (0) is also invalid in a Query packet — it would cause
    ClientInfo::read to skip parsing and bypass settings constraints.
    """
    for qk in [0, 3, 120, 255]:
        ok, msg = run_query(host, port, query_kind=qk)
        assert not ok, f"query_kind={qk} should be rejected, got ok"
        assert "Unexpected query kind" in msg, f"query_kind={qk}: {msg}"
    print("invalid query_kind values are rejected")


def test_invalid_compression(host, port):
    """Invalid compression values must be rejected."""
    for comp in [2, 255]:
        ok, msg = run_query(host, port, compression=comp)
        assert not ok, f"compression={comp} should be rejected, got ok"
        assert "Unknown compression state" in msg, f"compression={comp}: {msg}"
    print("invalid compression values are rejected")


def main():
    host = CLICKHOUSE_HOST
    port = CLICKHOUSE_PORT

    test_valid_queries_still_work(host, port)
    test_invalid_stage(host, port)
    test_invalid_query_kind(host, port)
    test_invalid_compression(host, port)


if __name__ == "__main__":
    main()
