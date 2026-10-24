#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

# A chain of named records A_k { x: A_(k-1) }. Each one is defined inside its own array field, so the
# schema nests only a few levels deep, while the required last field A_n reaches the whole chain.
# A record then needs n + 2 bytes: `id`, one per empty array and the bottom of the chain, so the
# 2 records this block declares cannot fit in its 2 * (n + 1) bytes.
python3 -c "
import json, sys
n = 100000
def zz(v):
    v = (v << 1) ^ (v >> 63)
    v &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = v & 0x7f
        v >>= 7
        out.append(b | 0x80 if v else b)
        if not v:
            break
    return bytes(out)
def ab(b):
    return zz(len(b)) + b
fields = [{'name': 'id', 'type': 'long'},
          {'name': 'f1', 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'A1', 'fields': [{'name': 'v', 'type': 'long'}]}}}]
for k in range(2, n + 1):
    fields.append({'name': 'f%d' % k, 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'A%d' % k, 'fields': [{'name': 'x', 'type': 'A%d' % (k - 1)}]}}})
fields.append({'name': 'last', 'type': 'A%d' % n})
schema = json.dumps({'type': 'record', 'name': 'root', 'fields': fields}, separators=(',', ':')).encode()
meta = zz(2) + ab(b'avro.schema') + ab(schema) + ab(b'avro.codec') + ab(b'null') + zz(0)
sync = bytes(16)
payload = b'\x00' * (2 * (n + 1))
open(sys.argv[1], 'wb').write(b'Obj\x01' + meta + sync + zz(2) + zz(len(payload)) + payload + sync)
" "$DIR/deep.avro"

$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DIR/deep.avro', Avro, 'id Int64')" 2>&1 | grep -c -F 'in block header cannot fit in'

rm -rf "$DIR"
