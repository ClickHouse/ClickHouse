#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: Parquet schema conversion (SchemaConverter::processSubtree) recurses over nested
# groups. Its only guard counts definition levels (def), which are only incremented for OPTIONAL/
# REPEATED nodes - so a chain of REQUIRED (non-nullable) groups bypasses the def==255 limit and
# overflows the native stack. Such nesting must be rejected as TOO_DEEP_RECURSION, not crash.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# Craft a minimal Parquet file whose schema is N nested REQUIRED groups + one REQUIRED INT32 leaf.
# The flat thrift schema list is O(depth), so this avoids pyarrow's super-linear nested-type build.
# 60000 is far above the default max_parser_depth (1000) and deep enough to overflow the native
# stack in any build, so the rejection is build-independent.
python3 - 60000 "${TMP_DIR}/deep.parquet" <<'PYEOF'
import struct as st, sys
def varint(n):
    o = bytearray()
    while True:
        b = n & 0x7F; n >>= 7
        if n: o.append(b | 0x80)
        else: o.append(b); break
    return bytes(o)
def i(n): return varint(n << 1)            # zigzag for non-negative
def b_(x): return varint(len(x)) + x
def stru(fs):                              # thrift compact struct, ascending field ids
    o = bytearray(); last = 0
    for fid, ct, v in fs:
        o.append(((fid - last) << 4) | ct); o += v; last = fid
    o.append(0); return bytes(o)
def lst(es):                               # list<struct>
    o = bytearray(); n = len(es)
    if n <= 14: o.append((n << 4) | 12)
    else: o.append(0xF0 | 12); o += varint(n)
    for e in es: o += e
    return bytes(o)
N = int(sys.argv[1])
# SchemaElement fields: 1=type(INT32=1), 3=repetition_type(REQUIRED=0), 4=name, 5=num_children
root = stru([(4, 8, b_(b"schema")), (5, 5, i(1))])
grp  = stru([(3, 5, i(0)), (4, 8, b_(b"f")), (5, 5, i(1))])
leaf = stru([(1, 5, i(1)), (3, 5, i(0)), (4, 8, b_(b"leaf"))])
sch  = lst([root] + [grp] * N + [leaf])
# FileMetaData: 1=version, 2=schema, 3=num_rows, 4=row_groups(empty)
fmd  = stru([(1, 5, i(1)), (2, 9, sch), (3, 6, i(0)), (4, 9, lst([]))])
open(sys.argv[2], "wb").write(b"PAR1" + fmd + st.pack("<i", len(fmd)) + b"PAR1")
PYEOF

$CLICKHOUSE_LOCAL --query "DESCRIBE TABLE file('${TMP_DIR}/deep.parquet', Parquet) FORMAT Null" 2>&1 \
    | grep -qF 'TOO_DEEP_RECURSION' && echo 'parquet_required_depth OK' || echo 'parquet_required_depth FAIL'
