#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for OOB reads from a List/LargeList/FixedSizeList child whose declared length
# is inconsistent with the parent:
#   - an empty child with non-zero parent offsets (child length patched to 0) that bypassed the
#     ColumnArray empty-child consistency check;
#   - a non-empty child shorter than length*stride for a FixedSizeList (child 21 -> 10);
#   - a negative child FieldNode.length (-1) that reaches arrow's ArrayData::Slice.
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

def write_arrow(arr, name='arr'):
    tbl = pa.table({name: arr})
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return bytearray(buf.getvalue())

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

# FixedSizeList<int32>[7] and List<int32> with 3 parent rows; child FieldNode.length patched
# 21 -> 0. The declared last offset (21) exceeds the empty child allocation; the ColumnArray
# empty-child short-circuit on unpatched code silently produced offsets past the allocation.
arr_fsl = pa.array([[i]*7 for i in range(3)], type=pa.list_(pa.int32(), 7))
d_fsl = bytearray(write_arrow(arr_fsl))
assert patch_all_int64(d_fsl, 21, 0) >= 1, f"no int64=21 found in FSL file ({len(d_fsl)} bytes)"
open(f'{out}/fsl_empty_child.arrow', 'wb').write(d_fsl)

arr_list = pa.array([[i]*7 for i in range(3)], type=pa.list_(pa.int32()))
d_list = bytearray(write_arrow(arr_list))
assert patch_all_int64(d_list, 21, 0) >= 1, f"no int64=21 found in List file ({len(d_list)} bytes)"
open(f'{out}/list_empty_child.arrow', 'wb').write(d_list)

# FixedSizeList<int32>[7] with a non-empty child shorter than length*stride: child 21 -> 10.
# Flatten would slice values[0,21] out of a 10-element child; the pre-Flatten stride/size check
# rejects it before Arrow's ArrayData::Slice fails.
arr_f = pa.array([[i]*7 for i in range(3)], type=pa.list_(pa.int32(), 7))
d_f = bytearray(write_arrow(arr_f, name='x'))
assert patch_all_int64(d_f, 21, 10) >= 1, "no int64=21 found in FSL-short-child file"
open(f'{out}/fsl_short_child.arrow', 'wb').write(d_f)

# Negative child FieldNode.length (-1): Struct / List / LargeList / FixedSizeList would slice a
# negative-length array inside StructArray::field()/Flatten().
def patch_fieldnode_length(data, pattern_qs, q_index, value):
    pat = struct.pack('<' + 'q'*len(pattern_qs), *pattern_qs)
    idx = data.find(pat)
    assert idx >= 0, f"FieldNode pattern {pattern_qs} not found"
    data[idx + 8*q_index : idx + 8*(q_index+1)] = struct.pack('<q', value)
    return data

g_struct = pa.StructArray.from_arrays([pa.array([42], type=pa.int32())], names=['a'])
open(f'{out}/struct_child_neg_length.arrow', 'wb').write(
    patch_fieldnode_length(write_arrow(g_struct, name='s'), [1,0,1,0], 2, -1))
g_list = pa.array([[]], type=pa.list_(pa.int32()))
open(f'{out}/list_child_neg_length.arrow', 'wb').write(
    patch_fieldnode_length(write_arrow(g_list, name='x'), [1,0,0,0], 2, -1))
g_large = pa.array([[]], type=pa.large_list(pa.int32()))
open(f'{out}/largelist_child_neg_length.arrow', 'wb').write(
    patch_fieldnode_length(write_arrow(g_large, name='x'), [1,0,0,0], 2, -1))
g_fsl = pa.array([[42]], type=pa.list_(pa.int32(), 1))
open(f'{out}/fixedlist_child_neg_length.arrow', 'wb').write(
    patch_fieldnode_length(write_arrow(g_fsl, name='x'), [1,0,1,0], 2, -1))
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

check_incorrect_data fsl_empty_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/fsl_empty_child.arrow', Arrow)"

check_incorrect_data list_empty_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_empty_child.arrow', Arrow)"

check_incorrect_data fsl_short_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/fsl_short_child.arrow', Arrow)"

check_incorrect_data struct_child_neg_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/struct_child_neg_length.arrow', Arrow)"

check_incorrect_data list_child_neg_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_child_neg_length.arrow', Arrow)"

check_incorrect_data largelist_child_neg_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/largelist_child_neg_length.arrow', Arrow)"

check_incorrect_data fixedlist_child_neg_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/fixedlist_child_neg_length.arrow', Arrow)"
