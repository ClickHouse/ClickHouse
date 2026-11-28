#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

# Both files hold `id` and a field that is not read, so it is skipped. `chain`: a chain of named records
# A_k { x: A_(k-1) }, each defined inside its own array field. `list`: a recursive record L { next: [null, L] }.
function gen()
{
    python3 -c "
import json, sys
dst, mode, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
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
if mode == 'chain':
    fields = [{'name': 'id', 'type': 'long'},
              {'name': 'f1', 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'A1', 'fields': [{'name': 'v', 'type': 'long'}]}}}]
    for k in range(2, n + 1):
        fields.append({'name': 'f%d' % k, 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'A%d' % k, 'fields': [{'name': 'x', 'type': 'A%d' % (k - 1)}]}}})
    fields.append({'name': 'last', 'type': 'A%d' % n})
    payload = zz(42) + b'\x00' * n + zz(7)
else:
    fields = [{'name': 'id', 'type': 'long'},
              {'name': 'list', 'type': {'type': 'record', 'name': 'L', 'fields': [{'name': 'next', 'type': ['null', 'L']}]}}]
    payload = zz(42) + zz(1) * n + zz(0)
schema = json.dumps({'type': 'record', 'name': 'root', 'fields': fields}, separators=(',', ':')).encode()
meta = zz(2) + ab(b'avro.schema') + ab(schema) + ab(b'avro.codec') + ab(b'null') + zz(0)
sync = bytes(16)
open(dst, 'wb').write(b'Obj\x01' + meta + sync + zz(1) + zz(len(payload)) + payload + sync)
" "$@"
}

gen "$DIR/chain_40.avro" chain 40
gen "$DIR/chain_50000.avro" chain 50000
gen "$DIR/list_10.avro" list 10
gen "$DIR/list_100000.avro" list 100000

for f in chain_40 list_10; do
    $CLICKHOUSE_LOCAL -q "SELECT id FROM file('$DIR/$f.avro', Avro, 'id Int64')"
done

for f in chain_50000 list_100000; do
    (ulimit -s 1024; $CLICKHOUSE_LOCAL -q "SELECT id FROM file('$DIR/$f.avro', Avro, 'id Int64')" 2>&1) | grep -q -F 'TOO_DEEP_RECURSION' && echo "$f: too deep"
done

rm -rf "$DIR"
