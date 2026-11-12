#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

# A chain of named records A_k { x: A_(k-1) }. A stage of `stage` consecutive records is defined nested inside one array
# field, and its innermost record refers to the top of the previous stage by name. So the schema JSON nests 3 levels per
# record of a stage, well below its limit of 256 levels, while schema inference follows the references: the field of a stage
# that ends at A_k is k + 1 levels deep. With `stage` = 1 every record has a field of its own, which makes n columns of depth
# up to n: the number of their type nodes grows as n^2, and the work to register their subcolumns grows even faster, which
# is far more than the inference itself costs. A larger stage keeps the depth of the chain and has few deep columns.
function gen()
{
    python3 -c "
import json, sys
n = int(sys.argv[2])
stage = int(sys.argv[3])
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
def chain(lo, hi):
    t = {'type': 'record', 'name': 'A%d' % lo, 'fields': [{'name': 'x' if lo > 1 else 'v', 'type': 'A%d' % (lo - 1) if lo > 1 else 'long'}]}
    for k in range(lo + 1, hi + 1):
        t = {'type': 'record', 'name': 'A%d' % k, 'fields': [{'name': 'x', 'type': t}]}
    return {'type': 'array', 'items': t}
fields = [{'name': 'id', 'type': 'long'}]
lo = 1
while lo <= n:
    hi = min(lo + stage - 1, n)
    fields.append({'name': 'f%d' % hi, 'type': chain(lo, hi)})
    lo = hi + 1
fields.append({'name': 'last', 'type': ['null', 'A%d' % n]})
schema = json.dumps({'type': 'record', 'name': 'root', 'fields': fields}, separators=(',', ':')).encode()
meta = zz(2) + ab(b'avro.schema') + ab(schema) + ab(b'avro.codec') + ab(b'null') + zz(0)
sync = bytes(16)
payload = zz(0) + b'\x00' * (len(fields) - 2) + zz(0)
open(sys.argv[1], 'wb').write(b'Obj\x01' + meta + sync + zz(1) + zz(len(payload)) + payload + sync)
" "$@"
}

gen "$DIR/chain_40.avro" 40 1
gen "$DIR/chain_300.avro" 300 60
gen "$DIR/chain_10000.avro" 10000 60

$CLICKHOUSE_LOCAL -q "DESC file('$DIR/chain_40.avro', Avro)" | wc -l

# A chain deeper than the schema JSON limit but within the stack is inferred, as by the legacy reader: `last` holds all
# 300 records of the chain.
DESCRIBED=$($CLICKHOUSE_LOCAL -q "DESC file('$DIR/chain_300.avro', Avro)")
echo "$DESCRIBED" | wc -l
echo "$DESCRIBED" | grep '^last' | grep -o 'Tuple(' | wc -l

# Inference follows named references, so only the stack size guard stops this chain. Inference builds the type of every
# field before the one that overflows, so its cost grows with the stack size: a 1 MiB stack keeps this case small on every
# build.
(ulimit -s 1024; $CLICKHOUSE_LOCAL -q "DESC file('$DIR/chain_10000.avro', Avro)" 2>&1) | grep -q -F 'TOO_DEEP_RECURSION' && echo 'too deep'

rm -rf "$DIR"
