#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"

TIME_MILLIS_FILE="$TMP_DIR/time_millis.parquet"
TIME64_MILLIS_FILE="$TMP_DIR/time64_millis.parquet"
TIME64_MICROS_FILE="$TMP_DIR/time64_micros.parquet"

${CLICKHOUSE_CLIENT} --use_legacy_to_time 0 -q "SELECT toTime('14:30:25') AS t FORMAT Parquet" > "$TIME_MILLIS_FILE"
${CLICKHOUSE_CLIENT} --use_legacy_to_time 0 -q "SELECT toTime64('14:30:25.123', 3) AS t FORMAT Parquet" > "$TIME64_MILLIS_FILE"
${CLICKHOUSE_CLIENT} --use_legacy_to_time 0 -q "SELECT toTime64('14:30:25.123456', 6) AS t FORMAT Parquet" > "$TIME64_MICROS_FILE"

python3 <<EOF
import pyarrow.parquet as pq
import sys

def check(path, expected_physical, expected_unit):
    col = pq.ParquetFile(path).schema.column(0)
    if str(col.physical_type) != expected_physical:
        print(f"unexpected physical type for {path}: {col.physical_type}, expected {expected_physical}", file=sys.stderr)
        sys.exit(1)
    logical = str(col.logical_type)
    if expected_unit not in logical:
        print(f"unexpected logical time unit for {path}: {logical}, expected {expected_unit}", file=sys.stderr)
        sys.exit(1)

check("$TIME_MILLIS_FILE", "INT32", "milliseconds")
check("$TIME64_MILLIS_FILE", "INT32", "milliseconds")
check("$TIME64_MICROS_FILE", "INT64", "microseconds")
EOF

${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('$TIME_MILLIS_FILE', Parquet, 't Time')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('$TIME64_MILLIS_FILE', Parquet, 't Time64(3)')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('$TIME64_MICROS_FILE', Parquet, 't Time64(6)')"

# Values near the Int32 millis limit must fail instead of silently truncating.
${CLICKHOUSE_CLIENT} --use_legacy_to_time 0 -q "SELECT toTime('999:59:59') AS t FORMAT Parquet" 2>&1 | grep -o 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

rm -rf "$TMP_DIR"
