#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for heap out-of-bounds reads in Arrow IPC format readers
# (per-element Value() readers — checkedCast validates before the loop).
#
#   8.  readColumnWithBooleanData      – BooleanArray::Value() / bit-packed buffer
#   9.  readColumnWithDate32Data (slow)– Date32Array::Value()
#   10. readColumnWithDate64Data       – Date64Array::Value()
#   11. readColumnWithTimestampData    – TimestampArray::Value()
#   12. readColumnWithTimeData (Time32)– Time32Array::Value()
#   13. readColumnWithTimeData (Time64)– Time64Array::Value()
#   14. readColumnWithFloat16Data      – HalfFloatArray::Value()
#   15. readColumnWithDecimalDataImpl  – DecimalArray::Value()
#   16. readColumnWithUUIDFromFixedBinaryData – FixedSizeBinaryArray::GetValue()

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
LARGE_N = 16384

def write_arrow(arr, name='x'):
    tbl = pa.table({name: arr})
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return bytearray(buf.getvalue())

def inflate_row_count(data, orig_n, large_n=LARGE_N):
    needle = struct.pack('<q', orig_n)
    patch  = struct.pack('<q', large_n)
    pos = 0
    while True:
        idx = data.find(needle, pos)
        if idx < 0:
            break
        if idx % 8 == 0:
            data[idx:idx+8] = patch
        pos = idx + 1
    return data

# 8. Boolean: 1 row → 1-byte bit buffer, inflated to 16384 rows
d = write_arrow(pa.array([True], type=pa.bool_()))
open(f'{out}/bool.arrow', 'wb').write(inflate_row_count(d, 1))

# 9. Date32 slow path (no numeric type hint → check_date_range=true → Value() loop)
d = write_arrow(pa.array([100], type=pa.date32()))
open(f'{out}/date32_slow.arrow', 'wb').write(inflate_row_count(d, 1))

# 10. Date64: 1 row → 8-byte body, inflated to 16384 rows
d = write_arrow(pa.array([1000000], type=pa.date64()))
open(f'{out}/date64.arrow', 'wb').write(inflate_row_count(d, 1))

# 11. Timestamp: 1 row → 8-byte body, inflated to 16384 rows
d = write_arrow(pa.array([1000000], type=pa.timestamp('us')))
open(f'{out}/timestamp.arrow', 'wb').write(inflate_row_count(d, 1))

# 12. Time32: 1 row → 4-byte body, inflated to 16384 rows
d = write_arrow(pa.array([3600000], type=pa.time32('ms')))
open(f'{out}/time32.arrow', 'wb').write(inflate_row_count(d, 1))

# 13. Time64: 1 row → 8-byte body, inflated to 16384 rows
d = write_arrow(pa.array([3600000000], type=pa.time64('us')))
open(f'{out}/time64.arrow', 'wb').write(inflate_row_count(d, 1))

# 14. Float16: 1 row → 2-byte body, inflated to 16384 rows
d = write_arrow(pa.array([1.0], type=pa.float16()))
open(f'{out}/float16.arrow', 'wb').write(inflate_row_count(d, 1))

# 15. Decimal128: 1 row → 16-byte body, inflated to 16384 rows
d = write_arrow(pa.array([pa.scalar(1, type=pa.decimal128(18, 4))]))
open(f'{out}/decimal.arrow', 'wb').write(inflate_row_count(d, 1))

# 16. UUID via FixedSizeBinary(16): 1 row → 16-byte body, inflated to 16384 rows
#     Query must specify 'x UUID' to route through readColumnWithUUIDFromFixedBinaryData.
d = write_arrow(pa.array([b'\xaa'*16], type=pa.binary(16)))
open(f'{out}/uuid.arrow', 'wb').write(inflate_row_count(d, 1))
PYEOF

check() {
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

check bool         $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/bool.arrow', Arrow)"
# Date32 slow path: no numeric type hint → check_date_range=true → Value() loop
check date32_slow  $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/date32_slow.arrow', Arrow)"
check date64       $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/date64.arrow', Arrow)"
check timestamp    $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/timestamp.arrow', Arrow)"
check time32       $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/time32.arrow', Arrow)"
check time64       $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/time64.arrow', Arrow)"
check float16      $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/float16.arrow', Arrow)"
check decimal      $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/decimal.arrow', Arrow)"
# UUID: explicit type hint routes through readColumnWithUUIDFromFixedBinaryData
check uuid         $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/uuid.arrow', Arrow, 'x UUID')"
