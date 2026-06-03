#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for heap out-of-bounds reads in Arrow IPC format readers
# (bulk memcpy readers — fixed-size buffers[1]).
#
# Each test crafts an Arrow IPC file where the RecordBatch declares many rows
# but the data buffer in the file body holds only a single element.  On an
# unpatched build this causes a heap-buffer-overflow caught by ASan.  The
# fixed build must reject every such file with INCORRECT_DATA.
#
#   1. readColumnWithNumericData                   – plain numeric (UInt32 / Int64 / …)
#   2. readColumnWithDurationData                  – Arrow Duration → Interval
#   3. readColumnWithFixedStringData               – FixedSizeBinary → FixedString
#   4. readColumnWithBigIntegerFromFixedBinaryData – FixedSizeBinary → Int128/Int256
#   5. readColumnWithDate32Data (fast path)        – Date32 → numeric (no range check)
#   6. readColumnWithIndexesDataImpl               – Dictionary index buffer
#   7. readIPv4ColumnWithInt32Data                 – Int32 → IPv4

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

# 1. Numeric: 1 row of UInt32 → 4-byte body, inflated to 16384 rows
d = write_arrow(pa.array([0xDEAD], type=pa.uint32()))
open(f'{out}/numeric.arrow', 'wb').write(inflate_row_count(d, 1))

# 2. Duration: 1 row → 8-byte body, inflated to 16384 rows
d = write_arrow(pa.array([12345], type=pa.duration('us')))
open(f'{out}/duration.arrow', 'wb').write(inflate_row_count(d, 1))

# 3. FixedString: FixedSizeBinary(8), 1 row → 8-byte body, inflated to 16384 rows
d = write_arrow(pa.array([b'\xCC'*8], type=pa.binary(8)))
open(f'{out}/fixed_string.arrow', 'wb').write(inflate_row_count(d, 1))

# 4. Int128: FixedSizeBinary(16), 1 row → 16-byte body, inflated to 16384 rows
d = write_arrow(pa.array([b'\xCC'*16], type=pa.binary(16)))
open(f'{out}/int128.arrow', 'wb').write(inflate_row_count(d, 1))

# 5. Date32 fast path (numeric type hint bypasses the range-check slow path)
d = write_arrow(pa.array([100], type=pa.date32()))
open(f'{out}/date32.arrow', 'wb').write(inflate_row_count(d, 1))

# 6. Dictionary indexes: integer-valued dictionary isolates the index-buffer path.
#    2 rows → 8-byte index body, inflated to 16384 rows.
d = write_arrow(pa.DictionaryArray.from_arrays(
    pa.array([0, 1], type=pa.int32()),
    pa.array([100, 200], type=pa.int32())))
open(f'{out}/dict_indexes.arrow', 'wb').write(inflate_row_count(d, 2))

# 7. IPv4: Arrow Int32, 1 row → 4-byte body, inflated to 16384 rows
d = write_arrow(pa.array([0x01020304], type=pa.int32()))
open(f'{out}/ipv4.arrow', 'wb').write(inflate_row_count(d, 1))
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

check numeric      $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/numeric.arrow', Arrow)"
check duration     $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/duration.arrow', Arrow)"
check fixed_string $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/fixed_string.arrow', Arrow)"
check int128       $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/int128.arrow', Arrow)"
# Date32 fast path: numeric type hint skips range checking, hits the raw buffer read
check date32       $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/date32.arrow', Arrow, 'x Int32')"
# Dict indexes: integer-valued dictionary isolates the index-buffer read path
check dict_indexes $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/dict_indexes.arrow', Arrow)"
# IPv4: Arrow Int32 with CH IPv4 type hint routes through readIPv4ColumnWithInt32Data
check ipv4         $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/ipv4.arrow', Arrow, 'x IPv4')"
