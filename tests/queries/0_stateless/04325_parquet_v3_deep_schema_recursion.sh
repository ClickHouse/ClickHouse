#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Parquet format is not available in the fast test build.

# Regression test for unbounded recursion in the Parquet V3 native reader's schema converter.
# A chain of nested REQUIRED groups maps to nested Tuples and recurses through
# SchemaConverter::processSubtree per level. The definition-level cap (255) only counts
# OPTIONAL/REPEATED nesting, so a deep REQUIRED chain recursed without bound and overflowed the
# stack -> uncatchable SIGSEGV (server crash / DoS), triggerable even by schema inference (DESC).
# checkStackSize() now turns it into a catchable TOO_DEEP_RECURSION exception.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

# Hand-build a Parquet file (PAR1 + FileMetaData footer, no row groups) whose schema is a root with
# N nested REQUIRED groups (num_children=1 each) and an INT32 leaf. Pure stdlib, no pyarrow.
python3 - "$WORK_DIR" <<'PYEOF'
import struct, sys
work = sys.argv[1]

class W:
    def __init__(s): s.out = bytearray()
    def u8(s, v): s.out.append(v & 0xff)
    def varint(s, v):
        v &= (1 << 64) - 1
        while True:
            b = v & 0x7f; v >>= 7
            s.out.append(b | 0x80) if v else s.out.append(b)
            if not v: return
    def zz(s, n): s.varint(((n << 1) ^ (n >> 63)) & ((1 << 64) - 1))

CT_I32, CT_I64, CT_BIN, CT_LIST, CT_STRUCT = 5, 6, 8, 9, 12

def field(w, last, fid, ctype):
    d = fid - last
    if 1 <= d <= 15: w.u8((d << 4) | ctype)
    else: w.u8(ctype); w.zz(fid)
    return fid

def schema_elem(w, name, repetition=None, num_children=None, ptype=None):
    last = 0
    if ptype is not None:
        last = field(w, last, 1, CT_I32); w.zz(ptype)          # type
    if repetition is not None:
        last = field(w, last, 3, CT_I32); w.zz(repetition)     # repetition_type (REQUIRED=0)
    last = field(w, last, 4, CT_BIN); w.varint(len(name)); w.out += name.encode()  # name
    if num_children is not None:
        last = field(w, last, 5, CT_I32); w.zz(num_children)   # num_children
    w.u8(0)

def build(n):
    fmd = W(); last = 0
    last = field(fmd, last, 1, CT_I32); fmd.zz(2)              # version
    last = field(fmd, last, 2, CT_LIST)                       # schema list<struct>
    total = n + 2                                             # root + n groups + leaf
    if total < 15: fmd.u8((total << 4) | CT_STRUCT)
    else: fmd.u8((15 << 4) | CT_STRUCT); fmd.varint(total)
    schema_elem(fmd, "root", repetition=None, num_children=1)
    for _ in range(n):
        schema_elem(fmd, "g", repetition=0, num_children=1)   # REQUIRED group
    schema_elem(fmd, "leaf", repetition=0, ptype=1)           # INT32 leaf
    last = field(fmd, last, 3, CT_I64); fmd.zz(0)             # num_rows
    last = field(fmd, last, 4, CT_LIST); fmd.u8((0 << 4) | CT_STRUCT)  # row_groups (empty)
    fmd.u8(0)
    meta = bytes(fmd.out)
    return b'PAR1' + meta + struct.pack('<I', len(meta)) + b'PAR1'

open(f"{work}/deep.parquet", "wb").write(build(50000))
open(f"{work}/shallow.parquet", "wb").write(build(8))
PYEOF

# Deeply nested schema must be rejected with a catchable error (not crash the process).
out=$(${CLICKHOUSE_LOCAL} --query "
    DESC file('${WORK_DIR}/deep.parquet', Parquet)
    SETTINGS input_format_parquet_use_native_reader_v3 = 1" 2>&1)
if echo "$out" | grep -q "TOO_DEEP_RECURSION"; then
    echo "deep: rejected"
else
    echo "deep: UNEXPECTED: $out"
fi

# A reasonably nested schema must still work.
shallow=$(${CLICKHOUSE_LOCAL} --query "
    DESC file('${WORK_DIR}/shallow.parquet', Parquet)
    SETTINGS input_format_parquet_use_native_reader_v3 = 1" 2>&1)
if echo "$shallow" | grep -q "Tuple"; then
    echo "shallow: ok"
else
    echo "shallow: UNEXPECTED: $shallow"
fi
