#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

python3 - <<'PY'
import concurrent.futures
import os
import random
import shlex
import socket
import struct
import subprocess

client_command = shlex.split(os.environ['CLICKHOUSE_CLIENT_BINARY'])
options = shlex.split(os.environ['CLICKHOUSE_CLIENT_OPT'])
options = [value for value in options if not value.startswith(('--host=', '--port=', '--send_logs_level=')) and value not in ('--secure', '--no-secure')]
backend = (os.environ['CLICKHOUSE_HOST'], int(os.environ['CLICKHOUSE_PORT_TCP']))
compressor = shlex.split(os.environ['CLICKHOUSE_COMPRESSOR'])


def read_exact(sock, count):
    data = bytearray()
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise RuntimeError('unexpected end of Hello')
        data.extend(part)
    return bytes(data)


def read_varuint(sock):
    data = bytearray()
    value = 0
    for shift in range(0, 70, 7):
        byte = read_exact(sock, 1)[0]
        data.append(byte)
        value |= (byte & 127) << shift
        if byte < 128:
            return value, bytes(data)
    raise RuntimeError('invalid Hello VarUInt')


def old_hello(sock):
    # The new revision adds no fields. Lower both Hello revisions to the immediately preceding
    # revision to exercise both sending directions' compatibility gates with the current binary.
    packet, data = read_varuint(sock)
    assert packet == 0
    length, encoded = read_varuint(sock)
    data += encoded + read_exact(sock, length)
    for _ in range(2):
        _, encoded = read_varuint(sock)
        data += encoded
    revision, _ = read_varuint(sock)
    assert revision >= 54493
    revision = 54492
    while revision >= 128:
        data += bytes([(revision & 127) | 128])
        revision >>= 7
    return data + bytes([revision])


def capture(query, extra=(), legacy=False):
    with socket.socket() as listener:
        listener.settimeout(10)
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        def relay():
            with listener.accept()[0] as client, socket.create_connection(backend) as server:
                client.settimeout(60)
                server.settimeout(60)
                def pump(source, target):
                    result = bytearray()
                    if legacy:
                        hello = old_hello(source)
                        target.sendall(hello)
                        result.extend(hello)
                    while data := source.recv(65536):
                        result.extend(data)
                        target.sendall(data)
                    target.shutdown(socket.SHUT_WR)
                    return bytes(result)
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    sent = pool.submit(pump, client, server)
                    received = pool.submit(pump, server, client)
                    return sent.result(), received.result()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            wire = pool.submit(relay)
            defaults = ['--no-secure', '--compression=1', '--proto_caps=notchunked', '--send_logs_level=none', '--send_profile_events=0', '--max_threads=1']
            overridden = {value.split('=')[0] for value in extra}
            defaults = [value for value in defaults if value.split('=')[0] not in overridden]
            result = subprocess.run(client_command + options + ['--host=127.0.0.1', f'--port={port}', *defaults,
                                    '--multiquery', '--query', query, *extra], capture_output=True, timeout=60)
            streams = wire.result(timeout=30)
    assert result.returncode == 0, result.stderr.decode()
    return result.stdout, streams


def frames(data):
    result = []
    offset = 0
    while offset + 25 <= len(data):
        method = data[offset + 16]
        compressed, uncompressed = struct.unpack_from('<II', data, offset + 17)
        if method not in (2, 0x82, 0x90) or not (9 < compressed < 2**22 and 0 < uncompressed <= 2**20) or offset + 16 + compressed > len(data):
            offset += 1
            continue
        candidate = data[offset:offset + 16 + compressed]
        decoded = subprocess.run(compressor + ['--decompress'], input=candidate, capture_output=True)
        if decoded.returncode:
            offset += 1
            continue
        assert len(decoded.stdout) == uncompressed
        result.append((method, uncompressed))
        offset += len(candidate)
    assert result, 'no valid compression frames captured'
    return result


query = "CREATE TEMPORARY TABLE native_small_frames (x UInt64); INSERT INTO native_small_frames VALUES (7); SELECT x FROM native_small_frames; SELECT toUInt64(number) FROM numbers(4096) FORMAT Null"
for codec, method in [('ZSTD', 0x90), ('LZ4', 0x82), ('LZ4HC', 0x82)]:
    output, streams = capture(query, [f'--network_compression_method={codec}'])
    assert output == b'7\n'
    for stream in streams:
        assert any(value == 2 for value, _ in frames(stream)), 'default small-frame bypass was not exercised'
    output, streams = capture('SELECT number FROM numbers(4096)', [f'--network_compression_method={codec}'])
    assert output == b''.join(f'{i}\n'.encode() for i in range(4096))
    server_frames = frames(streams[1])
    assert any(value == 2 for value, _ in server_frames)
    assert any(value == method for value, _ in server_frames)
    assert all(value == (2 if size < 128 else method) for value, size in server_frames)
    for legacy, extra in [(True, []), (False, ['--network_compression_min_bytes=0']), (False, ['--compatibility=26.8'])]:
        output, streams = capture('SELECT 7', [f'--network_compression_method={codec}', *extra], legacy=legacy)
        assert output == b'7\n'
        assert all(value == method for stream in streams for value, _ in frames(stream))
    print(codec, 'default mixed frames and compatibility passed')

output, _ = capture('SELECT 7', ['--compression=0'])
assert output == b'7\n'
print('compression disabled passed')

output, streams = capture('SELECT number FROM numbers(4096)', ['--network_compression_method=ZSTD', '--send_profile_events=1', '--send_logs_level=trace'])
assert output == b''.join(f'{i}\n'.encode() for i in range(4096))
assert {method for method, _ in frames(streams[1])} == {2, 0x90}
print('logs and ProfileEvents passed')

for threshold in (29, 30, 31):
    output, streams = capture('SELECT toUInt64(7) AS b', ['--network_compression_method=ZSTD', f'--network_compression_min_bytes={threshold}'])
    assert output == b'7\n'
    server_frames = frames(streams[1])
    assert any(size == 30 for _, size in server_frames)
    assert all(method == (2 if size < threshold else 0x90) for method, size in server_frames)
print('threshold boundary passed')

for compressible in (True, False):
    for payload_size in (127, 128, 129):
        value_size = payload_size - 31
        value = b'a' * value_size if compressible else random.Random(0).randbytes(value_size)
        query = f"SELECT CAST(unhex('{value.hex()}'), 'FixedString({value_size})') AS b FORMAT RawBLOB"
        output, streams = capture(query, ['--network_compression_method=ZSTD'])
        assert output == value
        server_frames = frames(streams[1])
        assert any(size == payload_size for _, size in server_frames)
        assert all(method == (2 if size < 128 else 0x90) for method, size in server_frames)
print('default boundary with compressible and incompressible values passed')

values = ','.join(f'({i})' for i in range(4096))
query = f'CREATE TEMPORARY TABLE native_small_frames_large (x UInt64); INSERT INTO native_small_frames_large VALUES {values}; SELECT sum(x) FROM native_small_frames_large'
output, streams = capture(query, ['--network_compression_method=ZSTD', '--async_insert=0', '--send_table_structure_on_insert_with_inline_data=1'])
assert output == f'{4095 * 4096 // 2}\n'.encode()
client_frames = frames(streams[0])
assert {method for method, _ in client_frames} == {2, 0x90}, client_frames
print('client mixed frames passed')

output, streams = capture('SELECT number FROM numbers(131072)', ['--network_compression_method=ZSTD', '--max_block_size=131072'])
assert output == b''.join(f'{i}\n'.encode() for i in range(131072))
server_frames = frames(streams[1])
full_frame = server_frames.index((0x90, 2**20))
assert server_frames[full_frame + 1] == (2, 29), server_frames
assert all(method == (2 if size < 128 else 0x90) for method, size in server_frames)
print('mixed frames within a large block passed')

output, streams = capture('SELECT number FROM numbers(0)', ['--network_compression_method=ZSTD'])
assert output == b''
assert {method for method, _ in frames(streams[1])} == {2}
print('empty result passed')

output, _ = capture('SELECT 7', ['--proto_caps=chunked', '--send_profile_events=1', '--send_logs_level=trace'])
assert output == b'7\n'
print('chunked protocol passed')
PY
