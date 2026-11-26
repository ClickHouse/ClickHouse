#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the native Arrow IPC reader's FixedSizeList handling of untrusted metadata:
# a non-positive `listSize` (negative wraps to a huge size_t when cast; zero makes the expected child
# length independent of the parent row count, leaving a forged row count unbounded). Both must be
# rejected as INCORRECT_DATA before any allocation, rather than driving an oversized column allocation.

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

# ArrowStream has a single schema message that precedes the record batch, so the first standalone int32
# `5` in the stream is the FixedSizeList `listSize`. Child values are far from 5 so the sentinel is unique
# to the schema; patch it to 0 (non-positive) and to -1 (negative) to exercise the rejection.
sch = pa.schema([pa.field('fsl', pa.list_(pa.int32(), 5))])
buf = io.BytesIO()
with ipc.new_stream(buf, sch) as w:
    w.write_table(pa.Table.from_arrays(
        [pa.array([[1000, 1001, 1002, 1003, 1004]], type=pa.list_(pa.int32(), 5))], schema=sch))
data = bytearray(buf.getvalue())
idx = data.find(struct.pack('<i', 5))
assert idx >= 0, "FixedSizeList listSize sentinel not found"

zero = bytearray(data)
zero[idx:idx + 4] = struct.pack('<i', 0)
open(f'{out}/list_size_zero.arrows', 'wb').write(zero)

neg = bytearray(data)
neg[idx:idx + 4] = struct.pack('<i', -1)
open(f'{out}/list_size_negative.arrows', 'wb').write(neg)
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

check_incorrect_data list_size_zero \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_size_zero.arrows', ArrowStream)"

check_incorrect_data list_size_negative \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/list_size_negative.arrows', ArrowStream)"
