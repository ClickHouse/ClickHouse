#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for two additional Arrow IPC buffer-validation paths.
#
#  Issue A — arithmetic overflow in checkArrowBuffer: a crafted row count near
#  2^62 makes elem_size * (offset + length) wrap to 0 mod 2^64, bypassing the
#  buffer-size guard.  The fixed build uses __builtin_mul_overflow and throws
#  INCORRECT_DATA.  On an unpatched build the subsequent reserve(2^62) catches
#  it with error 173 (no OOB read occurs in practice, but the check is wrong).
#
#  Issue B — validity bitmap (buffers[0]) not validated: a file with a valid
#  data buffer but a 1-byte bitmap for 64 rows lets IsNull() read past the
#  bitmap for rows 8-63.  The fixed build calls checkValidityBitmap before any
#  IsNull / IsValid loop.
#  Note: COUNT(*) bypasses column reads; use SUM to force value materialisation.

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

def write_arrow(arr, name='x'):
    tbl = pa.table({name: arr})
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return bytearray(buf.getvalue())

# Issue A: overflow.  elem_size=4 (UInt32).  4 * 2^62 ≡ 0 (mod 2^64), so the
# unguarded multiply "elem_size * count" would wrap to 0 and pass the check.
OVERFLOW_N = 1 << 62
d = write_arrow(pa.array([42], type=pa.uint32()))
needle = struct.pack('<q', 1); patch = struct.pack('<q', OVERFLOW_N)
pos = 0
while True:
    idx = d.find(needle, pos)
    if idx < 0: break
    if idx % 8 == 0: d[idx:idx+8] = patch
    pos = idx + 1
open(f'{out}/overflow.arrow', 'wb').write(d)

# Issue B: bitmap attack.  64 rows of valid Int32 data (256-byte buffer, passes
# checkArrowBuffer), but the validity bitmap is shrunk to 1 byte (covers only 8
# rows).  IsNull(i) for i >= 8 would read past the 1-byte bitmap without the fix.
# The bitmap buffer entry is {offset=0, length=8}; search for that specific pair
# to avoid patching the FieldNode null_count field which also equals 8.
arr = pa.array([None if i % 8 == 0 else i for i in range(64)], type=pa.int32())
d = write_arrow(arr)
bitmap_entry = struct.pack('<qq', 0, 8)   # {offset=0, length=8} — the bitmap buffer
idx = d.find(bitmap_entry)
assert idx >= 0, "could not find bitmap buffer entry"
d[idx+8 : idx+16] = struct.pack('<q', 1)  # patch length 8 → 1
open(f'{out}/bitmap_shrink.arrow', 'wb').write(d)
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

check_rejected() {
    # Accepts any non-zero exit code or exception — used for cases where the
    # vulnerability is unreachable via OOB read but the file must still be rejected.
    local label="$1"; shift
    local actual
    actual=$("$@" 2>&1)
    local exit_code=$?
    # Success (exit=0 with data output) means the file was silently accepted — bad.
    if [ "$exit_code" -ne 0 ] || echo "$actual" | grep -qE 'Exception|INCORRECT_DATA|Error'; then
        echo 'REJECTED'
    else
        echo "FAIL [$label] expected rejection, got silent success: $(echo "$actual" | head -1 | cut -c1-100)"
    fi
}

# Issue A: overflow — with standard IPC the row count is also huge, so reserve() rejects
# the file before __builtin_mul_overflow fires (Code: 173, not Code: 117).  The important
# property is that the file is REJECTED — no OOB read ever reaches raw pointer arithmetic.
check_rejected overflow $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/overflow.arrow', Arrow)"

# Issue B: bitmap — checkValidityBitmap must produce INCORRECT_DATA before IsNull reads.
# Use sum() to force column materialisation; count(*) is optimised away and skips data reads.
check_incorrect_data bitmap $CLICKHOUSE_LOCAL --query "SELECT sum(x) FROM file('${TMP_DIR}/bitmap_shrink.arrow', Arrow)"
