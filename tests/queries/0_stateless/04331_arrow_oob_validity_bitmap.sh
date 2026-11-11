#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for OOB reads of an Arrow validity bitmap:
#   - a truncated child bitmap after Flatten/Slice: a sliced child has an unknown null_count, and
#     arrow::ChunkedArray's constructor scans the (shrunk) bitmap (List(Nullable(int32)) child
#     bitmap 16 -> 1 byte; Struct field b sliced with bitmap 9 -> 1 byte);
#   - an unknown FieldNode.null_count (-1) over a forged-huge length with a 1-byte bitmap, where
#     building the arrow Table scans the bitmap over the declared length (heap OOB in CountSetBits).
# Each must be rejected as INCORRECT_DATA before any null_count scan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import struct, io, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]

def write_arrow(arr, name='arr'):
    tbl = pa.table({name: arr})
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return bytearray(buf.getvalue())

def shrink_buffer_len(data, old_len, new_len):
    """Shrink the first buffer-length field equal to old_len (a Buffer in the IPC metadata is
    {offset:int64, length:int64}, so the length is preceded by a valid offset)."""
    needle = struct.pack('<q', old_len)
    pos = 0
    while True:
        idx = data.find(needle, pos)
        if idx < 0:
            return False
        if idx >= 8:
            prev = struct.unpack_from('<q', data, idx - 8)[0]
            if 0 <= prev <= len(data):
                data[idx:idx+8] = struct.pack('<q', new_len)
                return True
        pos = idx + 1

# List(Nullable(int32)) with 129 child values: list offsets [1,129] make Flatten return a sliced
# nullable child whose 16-byte validity bitmap buffer we shrink to 1 byte. Each plausible
# buffer-length entry is tried and verified (via pyarrow round-trip) to actually shrink the child
# bitmap, because alignment padding makes the length value ambiguous to locate by bytes alone.
child_e = pa.array([None if i % 7 == 0 else i for i in range(129)], type=pa.int32())
arr_e1 = pa.ListArray.from_arrays(pa.array([1, 129], type=pa.int32()), child_e)
base_e1 = bytearray(write_arrow(arr_e1, name='x'))
d_e1 = None
pos = 0
while True:
    idx = base_e1.find(struct.pack('<q', 16), pos)
    if idx < 0:
        break
    if idx >= 8 and 0 <= struct.unpack_from('<q', base_e1, idx - 8)[0] <= len(base_e1):
        cand = bytearray(base_e1)
        cand[idx:idx+8] = struct.pack('<q', 1)
        with ipc.open_file(pa.py_buffer(bytes(cand))) as reader:
            chunk0 = reader.read_all().column('x').chunks[0]
        bm = chunk0.values.buffers()[0]
        if bm is not None and bm.size == 1:
            d_e1 = cand
            break
    pos = idx + 1
assert d_e1 is not None, "could not locate child bitmap buffer in list-child-bitmap file"
open(f'{out}/list_child_bitmap.arrow', 'wb').write(d_e1)

# Struct<a:int32, b:Nullable(int32)> with 65 rows: field b's FieldNode.length is patched 65 -> 70
# so StructArray::field() Slice-clamps it (unknown null_count), and b's 9-byte bitmap is shrunk to 1.
a_e = pa.array(list(range(65)), type=pa.int32())
b_e = pa.array([None if i % 7 == 0 else i for i in range(65)], type=pa.int32())
d_e2 = bytearray(write_arrow(pa.StructArray.from_arrays([a_e, b_e], names=['a', 'b']), name='s'))
bnc = sum(1 for i in range(65) if i % 7 == 0)
idx_e2 = d_e2.find(struct.pack('<6q', 65,0, 65,0, 65,bnc))
assert idx_e2 >= 0, "could not find Struct FieldNode pattern in struct-bitmap file"
d_e2[idx_e2+32:idx_e2+40] = struct.pack('<q', 70)  # force field b slice
assert shrink_buffer_len(d_e2, 9, 1), "no field b bitmap buffer (length 9) found in struct-bitmap file"
open(f'{out}/struct_child_bitmap.arrow', 'wb').write(d_e2)

# Unknown FieldNode.null_count (-1) over a forged-huge declared length with a 1-byte bitmap.
# Building the arrow Table computes the unknown null_count by scanning the bitmap over the
# declared length (heap OOB read in CountSetBits).
def forge_unknown_nullcount(arr, declared_length):
    data = write_arrow(arr, name='x')
    ones = [i for i in range(0, len(data) - 7, 8) if struct.unpack_from('<q', data, i)[0] == 1]
    for a in range(len(ones)):
        for b in range(a + 1, len(ones)):
            for nc in ones:
                if nc in (ones[a], ones[b]):
                    continue
                cand = bytearray(data)
                cand[ones[a]:ones[a]+8] = struct.pack('<q', declared_length)
                cand[ones[b]:ones[b]+8] = struct.pack('<q', declared_length)
                cand[nc:nc+8] = struct.pack('<q', -1)
                try:
                    with ipc.open_file(pa.py_buffer(bytes(cand))) as reader:
                        t = reader.read_all()  # does not touch null_count
                    if t.num_rows == declared_length and len(t.column('x').chunks[0]) == declared_length:
                        return cand
                except Exception:
                    pass
    raise RuntimeError("could not forge RecordBatch/FieldNode length + null_count markers")

open(f'{out}/int32_unknown_nullcount.arrow', 'wb').write(
    forge_unknown_nullcount(pa.array([None], type=pa.int32()), 1000))
open(f'{out}/list_unknown_nullcount.arrow', 'wb').write(
    forge_unknown_nullcount(pa.array([None], type=pa.list_(pa.int32())), 1000))
PYEOF

check_incorrect_data() {
    local label="$1"; shift
    local actual
    actual=$("$@" 2>&1)
    local exit_code=$?
    if echo "$actual" | grep -qF 'INCORRECT_DATA'; then
        echo 'INCORRECT_DATA'
    else
        local first_line
        first_line=$(echo "$actual" | head -1 | cut -c1-200)
        echo "FAIL [$label] expected INCORRECT_DATA (exit=${exit_code}); got: ${first_line:-<empty output>}"
    fi
}

check_incorrect_data list_child_bitmap \
    $CLICKHOUSE_LOCAL --query "SELECT sum(length(x)) FROM file('${TMP_DIR}/list_child_bitmap.arrow', Arrow)"

check_incorrect_data struct_child_bitmap \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/struct_child_bitmap.arrow', Arrow)"

check_incorrect_data int32_unknown_nullcount \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/int32_unknown_nullcount.arrow', Arrow)"

check_incorrect_data list_unknown_nullcount \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_unknown_nullcount.arrow', Arrow)"
