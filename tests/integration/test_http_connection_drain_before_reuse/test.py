import pytest
import time, http.client
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "instance",
    main_configs=[],
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def make_arrow_stream_data():
    # Produce a valid ~300 KB ArrowStream payload.
    # Made using `select 0x123456789abcdef0 as x from numbers(37000) settings output_format_arrow_compression_method='none' format ArrowStream`,
    # then finding all the parts of the hex dump that are not "f0debc9a78563412" (0x123456789abcdef0 in little endian).
    header = "ffffffff700000001000000000000a000c000600050008000a000000000104000c000000080008000000040008000000040000000100000014000000100014000800000007000c00000010001000000000000002100000001800000004000000000000000100000078000600080004000600000040000000ffffffff8800000014000000000000000c0016000600050008000c000c0000000003040018000000408404000000000000000a0018000c00040008000a0000003c00000010000000889000000000000000000000020000000000000000000000000000000000000000000000000000004084040000000000000000000100000088900000000000000000000000000000"
    footer = "ffffffff00000000"
    num_values = 37000

    return bytes.fromhex(header) + b"xxxxxxxx" * num_values + bytes.fromhex(footer)

def yield_then_sleep(data):
    yield data
    # Make the HTTP client wait after the data chunk but before sending the final "0\r\n\r\n" bytes.
    time.sleep(1)

# This used to break because the server didn't drain the final empty chunk from HTTP chunked encoded
# data, then tried to parse the next request from the same connection and misinterpreted the
# leftover empty chunk as part of next request's headers. Repro conditions:
#  * ArrowStream format has size in header, and our parser stops reading exactly after consuming all
#    payload bytes, without checking for eof after that. So the final empty chunk (end-of-data
#    indicator) doesn't get read by IInputFormat.
#  * Payload needs to be bigger than DBMS_DEFAULT_MAX_QUERY_SIZE (262144 bytes). Otherwise
#    executeQuery accidentally reads it while buffering the query.
def test_delay(started_cluster):
    try:
        node.query("create table test_delay (x Int64) engine Memory")

        conn = http.client.HTTPConnection(node.ip_address, 8123)
        data = make_arrow_stream_data()
        conn.request('POST', '/?query=insert%20into%20test_delay%20format%20ArrowStream', body=yield_then_sleep(data), headers={'Transfer-Encoding': 'chunked', 'Connection': 'keep-alive'}, encode_chunked=True)
        resp = conn.getresponse()
        #print(f"POST response headers: {resp.headers}")
        assert resp.status == 200
        body = resp.read()
        assert body == b""
        if (resp.getheader('Connection').lower() == 'close'):
            return
        assert resp.getheader('Connection').lower() == 'keep-alive'

        conn.request('GET', '/?query=select%2042')
        resp = conn.getresponse()
        assert resp.status == 200
        assert resp.read() == b"42\n"
    finally:
        node.query("drop table test_delay")


def test_form(started_cluster):
    conn = http.client.HTTPConnection(node.ip_address, 8123)

    boundary = "------------------------1234567890abcdef"

    body_parts = []
    body_parts.append(f'--{boundary}')
    body_parts.append('Content-Disposition: form-data; name="param_id"')
    body_parts.append('')
    body_parts.append('1')
    body_parts.append(f'--{boundary}--')

    body = '\r\n'.join(body_parts).encode('utf-8')

    headers = {
        'Content-Type': f'multipart/form-data; boundary={boundary}',
        'Content-Length': str(len(body)),
        'Connection': 'keep-alive'
    }

    query = "select%201%20as%20c%20where%20c%20%3D%20%7Bid%3AUInt8%7D"

    conn.request('POST', f'/?query={query}', body=body, headers=headers)
    resp = conn.getresponse()

    assert resp.status == 200
    assert resp.getheader('Connection').lower() == 'keep-alive'
    result = resp.read()
    assert result == b"1\n"

    conn.request('GET', '/?query=select%2042')
    resp = conn.getresponse()
    assert resp.status == 200
    assert resp.read() == b"42\n"


def test_invalid_data(started_cluster):
    try:
        node.query("create table test_invalid_data (x Int64) engine Memory")

        conn = http.client.HTTPConnection(node.ip_address, 8123)
        data = b"\0" * 300000
        conn.request('POST', '/?query=insert%20into%20test_invalid_data%20format%20ArrowStream', body=data, headers={'Content-Length': f'{len(data)}', 'Connection': 'keep-alive'})
        resp = conn.getresponse()
        assert resp.status != 200
        maybe_keepalive = resp.getheader('Connection')
        resp.read()
        if maybe_keepalive is not None and maybe_keepalive.lower() == 'keep-alive':
            conn.request('GET', '/?query=select%2042')
            resp = conn.getresponse()
            assert resp.status == 200
            assert resp.read() == b"42\n"
    finally:
        node.query("drop table test_invalid_data")
