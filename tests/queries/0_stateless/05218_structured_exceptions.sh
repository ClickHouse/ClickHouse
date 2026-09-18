#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

python3 - <<'PY'
import concurrent.futures
import io
import json
import os
import select
import shlex
import socket
import struct
import subprocess
import urllib.parse
import uuid

def without_options(args, names):
    result = []
    args = iter(args)
    for arg in args:
        if arg.split('=', 1)[0] in names:
            if '=' not in arg:
                next(args)
        else:
            result.append(arg)
    return result


curl = shlex.split(os.environ['CLICKHOUSE_CURL'])
client = without_options(shlex.split(os.environ['CLICKHOUSE_CLIENT']), {'--send_logs_level'})
url = os.environ['CLICKHOUSE_URL']
query_id = str(uuid.uuid4())
fields = {'code', 'name', 'code_name', 'query_id', 'query', 'formatted_message'}


def http(query, params=None, structured=True, get=False, compressed=False):
    params = dict(params or {})
    params['query_id'] = query_id
    args = curl + ['-sS', '--max-time', '30', '-D', '-']
    if structured:
        args += ['-H', 'X-ClickHouse-Exception-Format: JSON']
    if compressed:
        args += ['--compressed']
        params['enable_http_compression'] = 1
    if get:
        params['query'] = query
    else:
        args += ['--data-binary', '@-']
    args += [url + '&' + urllib.parse.urlencode(params)]
    data = query.encode() if isinstance(query, str) else query
    result = subprocess.run(args, input=None if get else data, capture_output=True, timeout=40)
    # A late exception deliberately leaves chunked transfer incomplete.
    assert result.returncode in (0, 18), result.stderr
    header, body = result.stdout.split(b'\r\n\r\n', 1)
    return header, body, result.returncode


def check(body, query, code=47):
    obj = json.loads(body) if isinstance(body, bytes) else body
    assert set(obj) == fields, obj
    assert obj['code'] == code, obj
    assert obj['name'] == 'DB::Exception', obj
    assert obj['query_id'] == query_id, obj
    assert obj['query'] == query, (obj['query'], query)
    assert obj['formatted_message'].startswith('Code: ' + str(code) + '.'), obj
    if code == 47:
        assert obj['code_name'] == 'UNKNOWN_IDENTIFIER', obj
    return obj


query = 'SELECT missing_structured_exception_column'
for get in (False, True):
    header, body, status = http(query, get=get)
    obj = check(body, query + ('\n' if get else ''))
    assert b'application/json' in header and status == 0, header
    _, plain, _ = http(query, structured=False, get=get)
    assert obj['formatted_message'] == plain.decode().removesuffix('\n'), (obj, plain)
print('HTTP GET and POST: OK')

query = 'SELECT missing_structured_exception_column /* "\\\n' + 'x' * 20000 + ' */'
_, body, _ = http(query, {'log_queries_cut_to_length': 1}, compressed=True)
check(body, query)
query = b'SELECT missing_structured_exception_column /* \xff */'
_, body, _ = http(query)
check(body, query.decode('utf-8', errors='replace'))
print('HTTP full query, escaping and compression: OK')

query = 'SELEC 1'
_, body, _ = http(query)
check(body, query, 62)
query = 'INSERT INTO missing_structured_exception_table FORMAT CSV\nsecret_insert_data'
_, body, _ = http(query)
obj = json.loads(body)
assert obj['code'] == 60, obj
assert obj['query'].startswith('INSERT INTO missing_structured_exception_table FORMAT CSV'), obj
assert 'secret_insert_data' not in obj['query'], obj
print('HTTP syntax error: OK')

query = 'SELECT {missing_structured_exception_parameter:UInt64}'
_, body, _ = http(query)
check(body, query, 456)
print('HTTP original parameterized query: OK')

query = 'SELECT missing_structured_exception_column'
_, body, _ = http(query, {'http_write_exception_in_output_format': 1, 'default_format': 'JSON'})
check(body, query)
_, body, _ = http(query, {'stacktrace': 1})
obj = check(body, query)
assert 'stack_trace' not in obj
print('HTTP output format and stack trace: OK')

for framing in ('JSONEachPacketString', 'JSONEachPacketBase64', 'EventStream'):
    _, body, _ = http(query, {'framing_output_format': framing, 'wait_end_of_query': 1})
    if framing.startswith('JSONEachPacket'):
        packets = [json.loads(line) for line in body.splitlines()]
        obj = next(p['exception'] for p in packets if p['packet'] == 'exception')
    else:
        event = next(p for p in body.split(b'\n\n') if p.startswith(b'event: exception\n'))
        obj = json.loads(event.split(b'data: ', 1)[1])['exception']
    check(obj, query)
print('HTTP framed errors: OK')

query = "SELECT number, throwIf(number = 10, 'structured error') FROM numbers(20)"
settings = {'max_threads': 1, 'max_block_size': 1, 'http_response_buffer_size': 1,
            'output_format_parallel_formatting': 0, 'wait_end_of_query': 0}
_, body, status = http(query, settings)
assert status == 18, (status, body)
parts = body.split(b'__exception__\r\n')
assert len(parts) == 3, body
obj = parts[1].split(b'\r\n', 1)[1].split(b'\n', 1)[0]
check(obj, query, 395)
settings['wait_end_of_query'] = 1
_, body, status = http(query, settings)
check(body, query, 395)
assert status == 0
settings['wait_end_of_query'] = 0
_, body, status = http(query + ' /* ' + 'x' * 20000 + ' */', settings)
assert status == 18 and b'__exception__' not in body, (status, body)
_, body, status = http('SELECT 42')
assert body.strip() == b'42' and status == 0, body
print('HTTP late and buffered execution errors: OK')

query = 'SELECT missing_structured_exception_column'
result = subprocess.run(client + ['--query_id', query_id, '--send_logs_level', 'none',
    '--query', query], capture_output=True, timeout=30)
assert result.returncode != 0 and b'UNKNOWN_IDENTIFIER' in result.stderr, result
# Reading a following result proves that the client consumed all three new fields.
result = subprocess.run(client + ['--multiquery', '--ignore-error', '--send_logs_level', 'none',
    '--query', 'CREATE TEMPORARY TABLE structured_exception_reuse (value UInt8); '
    'INSERT INTO structured_exception_reuse VALUES (42); ' + query
    + '; SELECT value FROM structured_exception_reuse'], capture_output=True, timeout=30)
assert result.stdout.strip() == b'42' and b'UNKNOWN_IDENTIFIER' in result.stderr, result
print('TCP exception and connection reuse: OK')

# A pre-extension client must receive exactly the legacy exception before its Pong.
def varuint(n):
    out = bytearray()
    while n >= 128:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)


def string(s):
    data = s.encode()
    return varuint(len(data)) + data


def readn(sock, size):
    out = bytearray()
    while len(out) < size:
        data = sock.recv(size - len(out))
        assert data, 'Unexpected EOF'
        out.extend(data)
    return bytes(out)


def readvar(sock):
    value = 0
    for shift in range(0, 64, 7):
        byte = readn(sock, 1)[0]
        value |= (byte & 127) << shift
        if byte < 128:
            return value
    raise AssertionError('Invalid VarUInt')


def readstr(sock):
    return readn(sock, readvar(sock))


with socket.create_connection((os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_TCP'])), timeout=30) as sock:
    # Revision 54000 predates `ClientInfo` and server timezone/display-name fields.
    sock.sendall(b'\0' + string('exception compatibility test') + varuint(26) + varuint(9)
                 + varuint(54000) + string(os.environ['CLICKHOUSE_DATABASE']) + string('default') + string(''))
    assert readvar(sock) == 0
    readstr(sock)
    for _ in range(3):
        readvar(sock)
    sock.sendall(b'\1' + string(query_id) + string('') + b'\2\0' + string(query))
    # Empty external-data block, including `BlockInfo`.
    sock.sendall(b'\2\0\1\0\2' + struct.pack('<i', -1) + b'\0\0\0')
    assert readvar(sock) == 2
    assert struct.unpack('<i', readn(sock, 4))[0] == 47
    assert readstr(sock) == b'DB::Exception'
    readstr(sock)
    readstr(sock)
    assert readn(sock, 1) == b'\0'
    sock.sendall(b'\4')
    assert readvar(sock) == 4
print('TCP legacy exception layout: OK')

# Inspect the new packet through a transparent proxy, while the real client handles
# handshake negotiation. Disable chunking so the exception's wire fields are visible.
def capture(listener):
    with listener.accept()[0] as downstream, socket.create_connection(
            (os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_TCP'])), timeout=30) as upstream:
        response = bytearray()
        while True:
            ready, _, _ = select.select([downstream, upstream], [], [], 30)
            assert ready, 'Proxy timed out'
            for source in ready:
                data = source.recv(65536)
                if not data:
                    return bytes(response)
                destination = upstream if source is downstream else downstream
                if source is upstream:
                    response.extend(data)
                destination.sendall(data)


proxy_client = without_options(client, {'--host', '--port'})

with socket.socket() as listener, concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    listener.settimeout(30)
    captured = executor.submit(capture, listener)
    result = subprocess.run(proxy_client + ['--host', '127.0.0.1', '--port', str(listener.getsockname()[1]),
        '--proto_caps', 'notchunked', '--query_id', query_id, '--send_logs_level', 'none', '--query', query],
        capture_output=True, timeout=30)
    assert result.returncode != 0 and b'UNKNOWN_IDENTIFIER' in result.stderr, result
    wire = captured.result(timeout=30)

prefix = b'\2' + struct.pack('<i', 47) + string('DB::Exception')
position = wire.index(prefix) + len(prefix)

class BufferSocket(io.BytesIO):
    recv = io.BytesIO.read

packet = BufferSocket(wire[position:])
readstr(packet)  # message
readstr(packet)  # stack trace
assert readn(packet, 1) == b'\0'  # obsolete nested flag
assert readstr(packet).decode() == query
assert readstr(packet) == b'UNKNOWN_IDENTIFIER'
assert readstr(packet).decode() == query_id
assert packet.read() == b''
print('TCP new exception fields: OK')

# The server revision is unknown on an authentication failure: even a new client
# must receive the legacy layout when the exception replaces `Hello`.
with socket.create_connection((os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_TCP'])), timeout=30) as sock:
    sock.sendall(b'\0' + string('exception authentication test') + varuint(26) + varuint(9)
                 + varuint(54493) + string('') + string('nonexistent_' + uuid.uuid4().hex) + string(''))
    assert readvar(sock) == 2
    assert struct.unpack('<i', readn(sock, 4))[0] != 0
    for _ in range(3):
        readstr(sock)
    assert readn(sock, 1) == b'\0'
    assert sock.recv(1) == b''
print('TCP handshake exception layout: OK')

PY
