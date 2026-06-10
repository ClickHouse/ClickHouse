#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: non-monotonic Arrow List offsets cause uint64 underflow in
# readOffsetsFromArrowListColumn, allowing a crafted file to bypass the
# ColumnArray constructor check (data->size() == last_offset) and construct a
# ColumnArray whose interior offsets point past the inner-column allocation.
#
# Critical PoC (inner_size=64, no Arrow allocation padding):
# Arrow offsets (int32): [0, 64, 65, 64]
#   Row 0: 64 elements (valid, fills the entire 64-byte allocation)
#   Row 1: starts at inner[64] — one byte past the end → OOB heap read
#   Row 2: 64-65 underflows to 2^64-1; cumulative = 65+(2^64-1) = 64 mod 2^64
#           last_offset=64 == inner_size=64 → ColumnArray constructor passes on unpatched code
# On the unpatched build, SELECT arrayElement(arr, 2) triggers an ASan abort.
#
# The same attack applies to LargeList (int64 offsets).

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
