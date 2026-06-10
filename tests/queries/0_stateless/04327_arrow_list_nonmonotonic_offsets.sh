#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for two classes of OOB reads in Arrow List/LargeList/FixedSizeList:
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
