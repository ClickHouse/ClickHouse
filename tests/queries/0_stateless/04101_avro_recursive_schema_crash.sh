#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for Avro infinite recursion crash.
# A crafted .avro file with a mutually recursive schema (TypeA -> TypeB -> TypeA)
# would cause avroNodeToDataType() to recurse infinitely, overflowing the stack
# and crashing the server with SIGSEGV.
# The fix detects cycles in the Avro schema and throws a user-friendly error.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Generate a malicious Avro file with a recursive schema.
# The schema defines TypeA with field b:TypeB, and TypeB with field a:TypeA (back-reference).
# The Avro C++ library resolves the symbolic reference into a direct pointer cycle.
AVRO_FILE="${CLICKHOUSE_TMP}/malicious_recursive.avro"

python3 -c "
import struct, os, json

def encode_long(n):
    n = (n << 1) ^ (n >> 63)
    result = b''
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            result += bytes([b | 0x80])
        else:
            result += bytes([b])
            break
    return result

def encode_bytes(data):
    return encode_long(len(data)) + data

def encode_string(s):
    return encode_bytes(s.encode() if isinstance(s, str) else s)

def encode_map(d):
    out = encode_long(len(d))
    for k, v in d.items():
        out += encode_string(k)
        out += encode_bytes(v)
    out += encode_long(0)
    return out

schema_json = json.dumps({
    'type': 'record',
    'name': 'TypeA',
    'fields': [{
        'name': 'b',
        'type': {
            'type': 'record',
            'name': 'TypeB',
            'fields': [{'name': 'a', 'type': 'TypeA'}]
        }
    }]
}).encode()

sync_marker = b'\\x1d\\xe3\\x80\\x44\\xf4\\x96\\xb5\\xd6\\xe6\\xa5\\x7b\\x18\\x63\\xb5\\x10\\x9a'

metadata = {'avro.schema': schema_json, 'avro.codec': b'null'}
header = b'Obj\\x01' + encode_map(metadata) + sync_marker
empty_block = encode_long(0) + encode_long(0) + sync_marker

with open('${AVRO_FILE}', 'wb') as f:
    f.write(header + empty_block)
"

# This query previously crashed the server. Now it should return an error.
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${AVRO_FILE}', 'Avro')" 2>&1 | grep -o "Recursive Avro schema is not supported"

# Also test via DESCRIBE to exercise avroNodeToDataType schema inference path
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${AVRO_FILE}', 'Avro')" 2>&1 | grep -o "Recursive Avro schema is not supported"

rm -f "${AVRO_FILE}"
