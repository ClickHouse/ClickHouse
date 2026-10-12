#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

# A chain of named records A_k { x: A_(k-1) }, each defined inside its own array field f_k, so the schema
# JSON nests only a few levels deep. Schema inference follows the references: f_k is 2k + 1 levels deep.
function gen()
{
    python3 -c "
import json, sys
n = int(sys.argv[2])
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
fields.append({'name': 'last', 'type': ['null', 'A%d' % n]})
schema = json.dumps({'type': 'record', 'name': 'root', 'fields': fields}, separators=(',', ':')).encode()
meta = zz(2) + ab(b'avro.schema') + ab(schema) + ab(b'avro.codec') + ab(b'null') + zz(0)
sync = bytes(16)
payload = zz(0) + b'\x00' * n + zz(0)
open(sys.argv[1], 'wb').write(b'Obj\x01' + meta + sync + zz(1) + zz(len(payload)) + payload + sync)
" "$@"
}

gen "$DIR/chain_40.avro" 40
gen "$DIR/chain_10000.avro" 10000

$CLICKHOUSE_LOCAL -q "DESC file('$DIR/chain_40.avro', Avro)" | wc -l

# Inference builds every field's type before the deepest one overflows, so its cost grows with the stack
# size: a 1 MiB stack keeps this case small on every build.
(ulimit -s 1024; $CLICKHOUSE_LOCAL -q "DESC file('$DIR/chain_10000.avro', Avro)" 2>&1) | grep -q -F 'TOO_DEEP_RECURSION' && echo 'too deep'

rm -rf "$DIR"
