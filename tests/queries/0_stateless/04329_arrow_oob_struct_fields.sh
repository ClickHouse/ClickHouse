#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for OOB reads from malformed Arrow Struct/Map columns:
#   - a struct field shorter than the parent struct (field length 5 -> 3), which arrow's
#     StructArray::field() silently Slice-clamps, leaving ColumnTuple fields of unequal size
#     (also covers Map value vs key length mismatch);
#   - a sliced struct whose child field is too short for the slice range (parent offsets [1,2]
#     over a 0-length field) and would slice child[1:2] inside ArrayData::Slice;
#   - a fields-less (zero-field) struct with a forged-huge length: with no field buffers its
#     length is backed only by the validity bitmap, and a nullable Tuple() wrapper would
#     materialize a length-sized null map.
# Each must be rejected as INCORRECT_DATA.

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
HUGE = 1 << 62

def write_arrow(arr, name='arr'):
    tbl = pa.table({name: arr})
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return bytearray(buf.getvalue())

def write_struct_schema(arr, name, nullable=True):
    sch = pa.schema([pa.field(name, arr.type, nullable=nullable)])
    buf = io.BytesIO()
    with ipc.new_file(buf, sch) as w:
        w.write_table(pa.Table.from_arrays([arr], schema=sch))
    return bytearray(buf.getvalue())

# Struct<a:int32,b:int32> with 5 rows; field b FieldNode.length patched 5 -> 3. The FieldNode
# vector is [struct(5,0), a(5,0), b(5,0)]; b's length is the 5th int64 (+32 from the vector start).
a_col = pa.array([1,2,3,4,5], type=pa.int32())
b_col = pa.array([10,20,30,40,50], type=pa.int32())
d_struct = write_arrow(pa.StructArray.from_arrays([a_col, b_col], names=['a','b']), name='s')
idx_struct = d_struct.find(struct.pack('<6q', 5,0, 5,0, 5,0))
assert idx_struct >= 0, "could not find Struct FieldNode pattern in struct file"
d_struct[idx_struct+32:idx_struct+40] = struct.pack('<q', 3)
open(f'{out}/struct_short_b.arrow', 'wb').write(d_struct)

# Map<int32,int32> with 1 row / 5 entries; value FieldNode.length patched 5 -> 3. The FieldNode
# vector is [map(1,0), entries(5,0), key(5,0), value(5,0)]; value's length is the 7th int64 (+48).
arr_map = pa.array([list(zip([1,2,3,4,5], [10,20,30,40,50]))], type=pa.map_(pa.int32(), pa.int32()))
d_map = write_arrow(arr_map, name='m')
idx_map = d_map.find(struct.pack('<8q', 1,0, 5,0, 5,0, 5,0))
assert idx_map >= 0, "could not find Map FieldNode pattern in map file"
d_map[idx_map+48:idx_map+56] = struct.pack('<q', 3)
open(f'{out}/map_value_short.arrow', 'wb').write(d_map)

# Sliced struct whose child field is too short for the slice range: the parent list/map slices
# the struct entries at offset 1 (offsets [1,2], entries length 2) while a struct child field
# length is 0, so StructArray::field() would slice child[1:2] out of a 0-length field.
def patch_struct_slice(arr, name, pattern_qs, struct_len_idx, child_len_idx, off_fmt):
    data = bytearray(write_arrow(arr, name=name))
    pat = struct.pack('<' + 'q'*len(pattern_qs), *pattern_qs)
    idx = data.find(pat)
    assert idx >= 0, f"FieldNode pattern {pattern_qs} not found"
    data[idx + 8*struct_len_idx : idx + 8*(struct_len_idx+1)] = struct.pack('<q', 2)  # entries length -> 2
    data[idx + 8*child_len_idx : idx + 8*(child_len_idx+1)] = struct.pack('<q', 0)    # child field length -> 0
    old = struct.pack(off_fmt, 0, 1)
    new = struct.pack(off_fmt, 1, 2)
    pos = 0
    while True:
        oi = data.find(old, pos)
        assert oi >= 0, "list offsets [0,1] not found"
        cand = bytearray(data)
        cand[oi:oi+len(old)] = new
        try:
            with ipc.open_file(pa.py_buffer(bytes(cand))) as reader:
                a = reader.read_all().column(name).chunks[0]
            if a.offsets.to_pylist() == [1, 2]:
                return cand
        except Exception:
            pass
        pos = oi + 1

open(f'{out}/list_struct_offset_child_zero.arrow', 'wb').write(
    patch_struct_slice(pa.array([[{'a': 20}]], type=pa.list_(pa.struct([('a', pa.int32())]))), 'x', [1,0,1,0,1,0], 2, 4, '<2i'))
open(f'{out}/largelist_struct_offset_child_zero.arrow', 'wb').write(
    patch_struct_slice(pa.array([[{'a': 20}]], type=pa.large_list(pa.struct([('a', pa.int32())]))), 'x', [1,0,1,0,1,0], 2, 4, '<2q'))
open(f'{out}/map_offset_key_zero.arrow', 'wb').write(
    patch_struct_slice(pa.array([[(1, 10)]], type=pa.map_(pa.int32(), pa.int32())), 'm', [1,0,1,0,1,0,1,0], 2, 4, '<2i'))

# Top-level fields-less Struct, RecordBatch/FieldNode length patched 5 -> 2^62 (no validity bitmap).
d = write_struct_schema(pa.array([{} for _ in range(5)], type=pa.struct([])), 's')
pos = [i for i in range(0, len(d) - 7, 8) if struct.unpack_from('<q', d, i)[0] == 5]
assert len(pos) >= 2, f"expected >=2 int64=5, got {len(pos)}"
for p in pos:
    d[p:p+8] = struct.pack('<q', HUGE)
open(f'{out}/empty_struct_huge_nullable.arrow', 'wb').write(d)

# List/LargeList<Struct<>> whose child empty-struct length and last offset are forged huge.
def write_list_empty_struct(list_type, off_fmt, length):
    d = bytearray(write_arrow(pa.array([[]], type=list_type), name='x'))
    fn = d.find(struct.pack('<4q', 1, 0, 0, 0))  # [list(1,0), struct(0,0)]
    assert fn >= 0, "list<struct<>> FieldNode pattern not found"
    d[fn+16:fn+24] = struct.pack('<q', length)
    old = struct.pack(off_fmt, 0, 0)
    pos = 0
    while True:
        oi = d.find(old, pos)
        assert oi >= 0, "list offsets [0,0] not found"
        cand = bytearray(d)
        cand[oi:oi+len(old)] = struct.pack(off_fmt, 0, length)
        try:
            with ipc.open_file(pa.py_buffer(bytes(cand))) as reader:
                a = reader.read_all().column('x').chunks[0]
            if a.offsets.to_pylist() == [0, length]:
                return cand
        except Exception:
            pass
        pos = oi + 1

open(f'{out}/list_empty_struct_huge_child.arrow', 'wb').write(
    write_list_empty_struct(pa.list_(pa.struct([])), '<2i', 1 << 30))
open(f'{out}/largelist_empty_struct_huge_child.arrow', 'wb').write(
    write_list_empty_struct(pa.large_list(pa.struct([])), '<2q', 1 << 62))

# FixedSizeList<Struct<>> with the schema list size and child FieldNode length patched huge.
FIXED = 1 << 30
df = bytearray(write_arrow(pa.array([[{}]], type=pa.list_(pa.struct([]), 1)), name='x'))
patched = None
for p in range(0, len(df) - 3):
    if struct.unpack_from('<i', df, p)[0] != 1:
        continue
    cand = bytearray(df)
    cand[p:p+4] = struct.pack('<i', FIXED)
    try:
        with ipc.open_file(pa.py_buffer(bytes(cand))) as reader:
            a = reader.read_all().column('x').chunks[0]
        if getattr(a.type, 'list_size', None) == FIXED:
            patched = cand
            break
    except Exception:
        pass
assert patched is not None, "FixedSizeList size marker not found"
fn = patched.find(struct.pack('<4q', 1, 0, 1, 0))  # [fixedlist(1,0), struct(1,0)]
assert fn >= 0, "fixedlist<struct<>> FieldNode pattern not found"
patched[fn+16:fn+24] = struct.pack('<q', FIXED)
open(f'{out}/fixedlist_empty_struct_huge_child.arrow', 'wb').write(patched)

# Fields-less Struct WITH a validity bitmap (null_count>0) but a forged-huge length; the null-map
# allocation must be rejected by validating the bitmap (too small) before allocating.
d = write_struct_schema(pa.array([{}, None, {}, None, {}], type=pa.struct([])), 's')
pos = [i for i in range(0, len(d) - 7, 8) if struct.unpack_from('<q', d, i)[0] == 5]
assert len(pos) >= 2
for p in pos:
    d[p:p+8] = struct.pack('<q', HUGE)
open(f'{out}/empty_struct_huge_nullcount.arrow', 'wb').write(d)
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

check_incorrect_data struct_short_b \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/struct_short_b.arrow', Arrow)"

check_incorrect_data map_value_short \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/map_value_short.arrow', Arrow)"

check_incorrect_data list_struct_offset_child_zero \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_struct_offset_child_zero.arrow', Arrow)"

check_incorrect_data largelist_struct_offset_child_zero \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/largelist_struct_offset_child_zero.arrow', Arrow)"

check_incorrect_data map_offset_key_zero \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/map_offset_key_zero.arrow', Arrow)"

check_incorrect_data empty_struct_huge_nullable \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/empty_struct_huge_nullable.arrow', Arrow)"

check_incorrect_data list_empty_struct_huge_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_empty_struct_huge_child.arrow', Arrow)"

check_incorrect_data largelist_empty_struct_huge_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/largelist_empty_struct_huge_child.arrow', Arrow)"

check_incorrect_data fixedlist_empty_struct_huge_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/fixedlist_empty_struct_huge_child.arrow', Arrow)"

check_incorrect_data empty_struct_huge_nullcount \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/empty_struct_huge_nullcount.arrow', Arrow)"
