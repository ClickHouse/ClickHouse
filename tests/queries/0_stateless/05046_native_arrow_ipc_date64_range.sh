#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format and pyarrow are not available in fasttest builds

# Arrow `date64` (milliseconds since the epoch) maps to DateTime (UInt32 seconds). A value outside
# the DateTime range used to be narrowed with a plain cast, silently wrapping modulo 2^32 into a
# wrong-but-plausible DateTime. It must instead throw by default and clamp under
# `date_time_overflow_behavior = 'saturate'`, like the date32 branch. The file is built with
# pyarrow because the ClickHouse writer never emits date64.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}.in_range.Arrow" "${DATA_FILE}.in_range.ArrowStream" "${DATA_FILE}.out_of_range.Arrow"' EXIT

python3 - "$DATA_FILE" <<'PY'
import sys
from datetime import date

import pyarrow as pa

base = sys.argv[1]
in_range = pa.table({
    'k': pa.array([1, 2, 3], type=pa.int64()),
    'd': pa.array([date(1970, 1, 4), date(2001, 2, 3), date(2105, 12, 31)], type=pa.date64()),
})
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.in_range.{fmt}", "wb") as sink:
        with opener(sink, in_range.schema) as writer:
            writer.write_table(in_range)
out_of_range = pa.table({
    'k': pa.array([1, 2, 3], type=pa.int64()),
    'd': pa.array([date(1969, 12, 31), date(2001, 2, 3), date(9999, 12, 31)], type=pa.date64()),
})
with pa.OSFile(f"{base}.out_of_range.Arrow", "wb") as sink:
    with pa.ipc.new_file(sink, out_of_range.schema) as writer:
        writer.write_table(out_of_range)
PY

for FMT in Arrow ArrowStream
do
    echo "--- ${FMT}: in-range date64 reads exactly ---"
    ${CLICKHOUSE_LOCAL} -q "SELECT k, d FROM file('${DATA_FILE}.in_range.${FMT}', '${FMT}') ORDER BY k SETTINGS session_timezone = 'UTC'"
done

echo "--- out-of-range date64 throws by default ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${DATA_FILE}.out_of_range.Arrow', 'Arrow') FORMAT Null" 2>&1 \
    | grep -oF 'is out of the allowed DateTime range' | head -1

echo "--- out-of-range date64 clamps with date_time_overflow_behavior = saturate ---"
${CLICKHOUSE_LOCAL} -q "
    SELECT k, d FROM file('${DATA_FILE}.out_of_range.Arrow', 'Arrow') ORDER BY k
    SETTINGS session_timezone = 'UTC', date_time_overflow_behavior = 'saturate'"
