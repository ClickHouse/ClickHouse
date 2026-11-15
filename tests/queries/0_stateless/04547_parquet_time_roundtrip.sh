#!/usr/bin/env bash
# Tags: no-fasttest
# Native Parquet writer emits logical TIME for Time/Time64; the native reader must infer
# Time64 (not DateTime64) and must not apply input_format_parquet_local_time_as_utc TZ shifts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/04547_parquet_time"
mkdir -p "$DATA_DIR"

${CLICKHOUSE_LOCAL} -q "SELECT toTime('12:00:00') AS t FORMAT Parquet" > "$DATA_DIR/time.parquet"
${CLICKHOUSE_LOCAL} -q "SELECT toTime64('12:00:00.000000', 6) AS t FORMAT Parquet" > "$DATA_DIR/time64_6.parquet"
${CLICKHOUSE_LOCAL} -q "SELECT toTime64('12:00:00.000000000', 9) AS t FORMAT Parquet" > "$DATA_DIR/time64_9.parquet"
# TIMESTAMP control: must still infer DateTime64.
${CLICKHOUSE_LOCAL} -q "SELECT toDateTime64('2023-01-01 12:00:00', 3, 'UTC') AS ts FORMAT Parquet" > "$DATA_DIR/timestamp.parquet"

echo '--- schema inference ---'
${CLICKHOUSE_LOCAL} -q "SELECT toTypeName(t), t FROM file('$DATA_DIR/time.parquet', Parquet)"
${CLICKHOUSE_LOCAL} -q "SELECT toTypeName(t), t FROM file('$DATA_DIR/time64_6.parquet', Parquet)"
${CLICKHOUSE_LOCAL} -q "SELECT toTypeName(t), t FROM file('$DATA_DIR/time64_9.parquet', Parquet)"
${CLICKHOUSE_LOCAL} -q "SELECT toTypeName(ts), ts FROM file('$DATA_DIR/timestamp.parquet', Parquet)"

echo '--- local_time_as_utc=0 must not shift TIME ---'
${CLICKHOUSE_LOCAL} -q "SELECT toTypeName(t), t FROM file('$DATA_DIR/time64_6.parquet', Parquet) SETTINGS input_format_parquet_local_time_as_utc=0, session_timezone='Europe/Amsterdam'"

echo '--- typed Time64 hint ---'
${CLICKHOUSE_LOCAL} -q "SELECT t FROM file('$DATA_DIR/time64_6.parquet', Parquet, 't Time64(6)') SETTINGS input_format_parquet_local_time_as_utc=0, session_timezone='Europe/Moscow'"
