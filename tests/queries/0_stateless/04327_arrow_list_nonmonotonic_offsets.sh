#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for OOB reads in Arrow List/LargeList/FixedSizeList/Struct/Map:
#
# Class A — non-monotonic offsets (uint64 underflow):
#   Arrow offsets [0,64,65,64] for List(UInt8) with inner_size=64.
#   Row 1 starts at inner[64] (one byte past the allocation); row 2's uint64
#   underflow wraps last_offset back to 64 == inner_size, bypassing the
#   ColumnArray constructor check on unpatched code.
#   SELECT arrayElement(arr,2) triggers an ASan abort on unpatched builds.
#   Applies to LargeList (int64 offsets) identically.
#
# Class B — empty child with non-zero offsets:
#   FixedSizeList<int32>[7] and List<int32> with 3 parent rows whose child
#   FieldNode.length is patched to 0.  The declared last offset (21) exceeds
#   the empty child allocation; ColumnArray's constructor skips the consistency
#   check when data->empty(), silently producing a column with all offsets past
#   the inner allocation.  SELECT * triggers an ASan abort on unpatched builds.
#
# Class C — Struct/Tuple field-length mismatch:
#   arrow::StructArray::field() silently Slice-clamps a child whose FieldNode.length
#   is shorter than the parent struct's length, so independently-read fields can have
#   different row counts.  ColumnTuple on unpatched code never validates this, letting
#   readers access the short field past its allocation.
#   Case C1: plain Struct<a:int32,b:int32> with 5 rows, field b patched 5→3.
#   Case C2: Map<int32,int32> with 1 row / 5 entries, value FieldNode.length patched 5→3.
#   SELECT * triggers an ASan abort on unpatched builds for both.
#
# Class D — decreasing offsets that reach Flatten():
#   List<int32> with offsets patched to [64,0,6].  arrow::ListArray::Flatten() slices
#   the values array from offset[0]=64 to offset[2]=6 before the offsets reader's
#   monotonicity check runs, failing deep inside Arrow's ArrayData::Slice (STD_EXCEPTION)
#   on code that validates monotonicity too late.  The pre-Flatten check rejects it as
#   INCORRECT_DATA.
#
# Class E — truncated child validity bitmap after Flatten/Slice:
#   Flatten()/StructArray::field() yield a child slice with kUnknownNullCount, so
#   arrow::ChunkedArray's constructor scans the child bitmap via null_count().  A truncated
#   bitmap must be validated (without calling null_count) before that scan.
#   Case E1: List(Nullable(int32)), child bitmap shrunk 9→1 byte.
#   Case E2: Struct<a,Nullable(b)>, field b sliced and its bitmap shrunk 9→1 byte.
#
# Class F — FixedSizeList child shorter than length*stride:
#   FixedSizeList<int32>[7] with 3 rows; child FieldNode.length patched 21→10.  The
#   pre-Flatten stride/size check rejects it before Arrow's ArrayData::Slice fails.

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

# Case 1: List(UInt8), int32 offsets.
# Inner size = 64 bytes (Arrow allocates exactly 64, no padding).
# Valid array has offsets [0,64,65,66]; patch last entry 66→64
# so offsets become [0,64,65,64]: row 1 starts at inner[64] (OOB),
# and the uint64 underflow at row 2 wraps last_offset back to 64 == inner_size.
arr = pa.array([[0xAA]*64, [0xBB], [0xCC]], type=pa.list_(pa.uint8()))
d = write_arrow(arr)
valid_last = struct.pack('<i', 66)   # offsets[3] = 64+1+1 = 66
malicious   = struct.pack('<i', 64)  # patch to 64 < 65
idx = d.rfind(valid_last)
assert idx >= 0 and idx % 4 == 0, f"could not find last offset int32=66 (file={len(d)} bytes)"
d[idx:idx+4] = malicious
open(f'{out}/list_nonmonotonic.arrow', 'wb').write(d)

# Case 2: LargeList(UInt8), int64 offsets — same OOB via 64-bit underflow.
arr64 = pa.array([[0xAA]*64, [0xBB], [0xCC]], type=pa.large_list(pa.uint8()))
d64 = write_arrow(arr64)
valid_last64 = struct.pack('<q', 66)
malicious64  = struct.pack('<q', 64)
idx64 = d64.rfind(valid_last64)
assert idx64 >= 0 and idx64 % 8 == 0, f"could not find last offset int64=66"
d64[idx64:idx64+8] = malicious64
open(f'{out}/largelist_nonmonotonic.arrow', 'wb').write(d64)

def patch_all_int64(data, old_val, new_val):
    """Patch all 8-byte-aligned occurrences of old_val (int64) to new_val."""
    needle = struct.pack('<q', old_val)
    replacement = struct.pack('<q', new_val)
    count = 0
    pos = 0
    while True:
        idx = data.find(needle, pos)
        if idx < 0:
            break
        if idx % 8 == 0:
            data[idx:idx+8] = replacement
            count += 1
        pos = idx + 1
    return count

# Case 3: FixedSizeList<int32>[7] with 3 parent rows; child FieldNode.length patched
# from 21 to 0.  ColumnArray::create's empty-child short-circuit on unpatched code
# silently constructs a column with offsets [0,7,14,21] pointing past the empty
# child allocation; SELECT * materialises the OOB data and triggers an ASan abort.
arr_fsl = pa.array([[i]*7 for i in range(3)], type=pa.list_(pa.int32(), 7))
d_fsl = bytearray(write_arrow(arr_fsl))
n = patch_all_int64(d_fsl, 21, 0)
assert n >= 1, f"no int64=21 found in FSL file ({len(d_fsl)} bytes)"
open(f'{out}/fsl_empty_child.arrow', 'wb').write(d_fsl)

# Case 4: List<int32> with 3 parent rows; child FieldNode.length patched from 21 to 0.
# Same bypass via ColumnArray empty-child short-circuit.
arr_list = pa.array([[i]*7 for i in range(3)], type=pa.list_(pa.int32()))
d_list = bytearray(write_arrow(arr_list))
n = patch_all_int64(d_list, 21, 0)
assert n >= 1, f"no int64=21 found in List file ({len(d_list)} bytes)"
open(f'{out}/list_empty_child.arrow', 'wb').write(d_list)

# Case C1: Struct<a:int32,b:int32> with 5 rows; field b FieldNode.length patched 5→3.
# The FieldNode vector for this schema is [struct(5,0), a(5,0), b(5,0)]; b's length
# is the 5th int64 (byte offset +32 from the start of the vector).
a_col = pa.array([1,2,3,4,5], type=pa.int32())
b_col = pa.array([10,20,30,40,50], type=pa.int32())
arr_struct = pa.StructArray.from_arrays([a_col, b_col], names=['a','b'])
d_struct = write_arrow(arr_struct, name='s')
pat_struct = struct.pack('<6q', 5,0, 5,0, 5,0)
idx_struct = d_struct.find(pat_struct)
assert idx_struct >= 0, "could not find Struct FieldNode pattern in struct file"
d_struct[idx_struct+32:idx_struct+40] = struct.pack('<q', 3)
open(f'{out}/struct_short_b.arrow', 'wb').write(d_struct)

# Case C2: Map<int32,int32> with 1 row / 5 entries; value FieldNode.length patched 5→3.
# The FieldNode vector is [map(1,0), entries(5,0), key(5,0), value(5,0)]; value's length
# is the 7th int64 (byte offset +48 from the start of the vector).
keys = [1,2,3,4,5]; vals = [10,20,30,40,50]
arr_map = pa.array([list(zip(keys,vals))], type=pa.map_(pa.int32(), pa.int32()))
d_map = write_arrow(arr_map, name='m')
pat_map = struct.pack('<8q', 1,0, 5,0, 5,0, 5,0)
idx_map = d_map.find(pat_map)
assert idx_map >= 0, "could not find Map FieldNode pattern in map file"
d_map[idx_map+48:idx_map+56] = struct.pack('<q', 3)
open(f'{out}/map_value_short.arrow', 'wb').write(d_map)

# Case D: List<int32> with a decreasing offset pair that reaches Arrow's Flatten()
# before the offsets reader runs.  Valid offsets [0,4,6] are patched to [64,0,6]:
# Flatten() slices the values array from offset[0]=64 to offset[2]=6, which fails
# deep inside Arrow's ArrayData::Slice (STD_EXCEPTION) when monotonicity is validated
# only later.  The pre-Flatten check rejects it as INCORRECT_DATA.
arr_decr = pa.array([[1,2,3,4],[5,6]], type=pa.list_(pa.int32()))  # offsets [0,4,6]
d_decr = bytearray(write_arrow(arr_decr))
needle_decr = struct.pack('<3i', 0, 4, 6)
idx_decr = d_decr.find(needle_decr)
assert idx_decr >= 0, "could not find List offsets [0,4,6] in decreasing-offset file"
d_decr[idx_decr:idx_decr+12] = struct.pack('<3i', 64, 0, 6)
open(f'{out}/list_decreasing_offset.arrow', 'wb').write(d_decr)

# Class E — truncated child validity bitmap after Flatten/Slice.
# Arrow's Flatten()/StructArray::field() produce a child slice with kUnknownNullCount;
# arrow::ChunkedArray's constructor then sums chunk->null_count(), which scans the
# child validity bitmap.  A truncated bitmap is read out of bounds there, before any
# later validation.  The bitmap must be validated (without calling null_count) first.
#
# Case E1: List(Nullable(int32)).  Child has 65 rows (9-byte bitmap); list offsets [1,65]
# make Flatten return a sliced nullable child.  The child bitmap buffer length is shrunk 9→1.
def shrink_buffer_len(data, old_len, new_len):
    """Shrink the first buffer-length field equal to old_len (a Buffer in the IPC
    metadata is {offset:int64, length:int64}, so the length is preceded by a valid offset)."""
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

# 129 child values: list offsets [1,129] make Flatten return a sliced nullable child whose
# 16-byte validity bitmap buffer we shrink to 1 byte.  We try each plausible buffer-length
# entry and verify (via pyarrow round-trip) that the child bitmap actually became 1 byte,
# because buffer alignment padding makes the length value ambiguous to locate by bytes alone.
child_e = pa.array([None if i % 7 == 0 else i for i in range(129)], type=pa.int32())
arr_e1 = pa.ListArray.from_arrays(pa.array([1, 129], type=pa.int32()), child_e)
base_e1 = bytearray(write_arrow(arr_e1, name='x'))
d_e1 = None
target = struct.pack('<q', 16)
pos = 0
while True:
    idx = base_e1.find(target, pos)
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

# Case E2: Struct<a:int32, b:Nullable(int32)> with 65 rows.  Field b's FieldNode.length is
# patched 65→70 so StructArray::field() Slice-clamps it (kUnknownNullCount), and b's 9-byte
# validity bitmap buffer is shrunk to 1 byte.
a_e = pa.array(list(range(65)), type=pa.int32())
b_e = pa.array([None if i % 7 == 0 else i for i in range(65)], type=pa.int32())
arr_e2 = pa.StructArray.from_arrays([a_e, b_e], names=['a', 'b'])
d_e2 = bytearray(write_arrow(arr_e2, name='s'))
bnc = sum(1 for i in range(65) if i % 7 == 0)
pat_e2 = struct.pack('<6q', 65,0, 65,0, 65,bnc)
idx_e2 = d_e2.find(pat_e2)
assert idx_e2 >= 0, "could not find Struct FieldNode pattern in struct-bitmap file"
d_e2[idx_e2+32:idx_e2+40] = struct.pack('<q', 70)  # force field b slice
assert shrink_buffer_len(d_e2, 9, 1), "no field b bitmap buffer (length 9) found in struct-bitmap file"
open(f'{out}/struct_child_bitmap.arrow', 'wb').write(d_e2)

# Class F — FixedSizeList<int32>[7] with a non-empty child shorter than length*stride.
# 3 parent rows need 21 child values; child FieldNode.length is patched 21→10.  Flatten()
# would slice values[0,21] out of a 10-element child; the pre-Flatten stride/size check
# rejects it as INCORRECT_DATA instead of failing inside Arrow's ArrayData::Slice.
arr_f = pa.array([[i]*7 for i in range(3)], type=pa.list_(pa.int32(), 7))
d_f = bytearray(write_arrow(arr_f, name='x'))
n = patch_all_int64(d_f, 21, 10)
assert n >= 1, "no int64=21 found in FSL-short-child file"
open(f'{out}/fsl_short_child.arrow', 'wb').write(d_f)
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

check_incorrect_data list_nonmonotonic \
    $CLICKHOUSE_LOCAL --query "SELECT arrayElement(arr, 2) FROM file('${TMP_DIR}/list_nonmonotonic.arrow', Arrow)"

check_incorrect_data largelist_nonmonotonic \
    $CLICKHOUSE_LOCAL --query "SELECT arrayElement(arr, 2) FROM file('${TMP_DIR}/largelist_nonmonotonic.arrow', Arrow)"

check_incorrect_data fsl_empty_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/fsl_empty_child.arrow', Arrow)"

check_incorrect_data list_empty_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_empty_child.arrow', Arrow)"

check_incorrect_data struct_short_b \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/struct_short_b.arrow', Arrow)"

check_incorrect_data map_value_short \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/map_value_short.arrow', Arrow)"

check_incorrect_data list_decreasing_offset \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_decreasing_offset.arrow', Arrow)"

check_incorrect_data list_child_bitmap \
    $CLICKHOUSE_LOCAL --query "SELECT sum(length(x)) FROM file('${TMP_DIR}/list_child_bitmap.arrow', Arrow)"

check_incorrect_data struct_child_bitmap \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/struct_child_bitmap.arrow', Arrow)"

check_incorrect_data fsl_short_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/fsl_short_child.arrow', Arrow)"
