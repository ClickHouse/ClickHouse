#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: non-monotonic Arrow List offsets cause uint64 underflow in
# readOffsetsFromArrowListColumn, allowing a crafted file to bypass the
# ColumnArray constructor check (data->size() == last_offset) and construct a
# ColumnArray whose interior offsets point past the inner-column allocation.
#
# PoC: List(UInt8) with 8 rows, inner size 32.
# Arrow offsets (int32): [0, 32, 33, 34, 35, 36, 37, 38, 32]
# Row 7 has offset 32 < 38, so elements = 32-38 underflows as uint64 to 2^64-6.
# Cumulative last CH offset = 38 + (2^64-6) = 32 mod 2^64 = 32 = inner size,
# which would pass the ColumnArray constructor check on unpatched code while
# rows 1-6 point 1..6 bytes past the inner allocation and row 7 has length 2^64-6.
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
# Build a valid 8-row array with row sizes [32,1,1,1,1,1,1,1] then patch
# the last offset from 39 to 32 — making it non-monotonic (32 < 38).
arr = pa.array([[0xAA]*32] + [[i] for i in range(7)], type=pa.list_(pa.uint8()))
d = write_arrow(arr)
valid_last = struct.pack('<i', 39)   # offsets[8] = 32+1+1+1+1+1+1+1 = 39
malicious   = struct.pack('<i', 32)  # patch to 32 < 38
idx = d.rfind(valid_last)
assert idx >= 0 and idx % 4 == 0, "could not find last offset int32=39"
d[idx:idx+4] = malicious
open(f'{out}/list_nonmonotonic.arrow', 'wb').write(d)

# Case 2: LargeList(UInt8), int64 offsets — same underflow via 64-bit subtraction.
arr64 = pa.array([[0xAA]*32] + [[i] for i in range(7)], type=pa.large_list(pa.uint8()))
d64 = write_arrow(arr64)
valid_last64 = struct.pack('<q', 39)
malicious64  = struct.pack('<q', 32)
idx64 = d64.rfind(valid_last64)
assert idx64 >= 0 and idx64 % 8 == 0, "could not find last offset int64=39"
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
    $CLICKHOUSE_LOCAL --query "SELECT length(arr) FROM file('${TMP_DIR}/list_nonmonotonic.arrow', Arrow)"

check_incorrect_data largelist_nonmonotonic \
    $CLICKHOUSE_LOCAL --query "SELECT length(arr) FROM file('${TMP_DIR}/largelist_nonmonotonic.arrow', Arrow)"
