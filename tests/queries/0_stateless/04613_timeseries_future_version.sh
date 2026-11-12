#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-replicated-database: `ATTACH TABLE` with an explicit UUID is not allowed there.
#
# A TimeSeries table with a version newer than the latest known one (possible after a downgrade of ClickHouse)
# can be attached, so that it can be inspected or dropped, but every query over it must be rejected
# with an instructive error (see TimeSeriesVersion.h).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

echo '--- a new definition with a future version is rejected, and a full-definition ATTACH of a TimeSeries table with inner tables is refused ---'
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "CREATE TABLE ts_future_version (metric_name String) ENGINE = TimeSeries SETTINGS version = 999" 2>&1 | grep -o "INVALID_SETTING_VALUE" | head -1
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "ATTACH TABLE ts_future_version UUID '$uuid' (metric_name String) ENGINE = TimeSeries SETTINGS version = 999" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo '--- a table with a future version can be attached, even if its definition uses a setting unknown to this server ---'
# The table is stored in a clickhouse-local data directory, its stored definition is rewritten to a future
# version, and the next start loads it.
local_query()
{
    $CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/04613_data" "$@"
}
local_query --allow_experimental_time_series_table=1 -q "CREATE TABLE ts_future_version (metric_name String) ENGINE = TimeSeries"
$CLICKHOUSE_LOCAL --param_metadata="${CLICKHOUSE_TMP:?}/04613_data/metadata/default/ts_future_version.sql" --param_copy="${CLICKHOUSE_TMP:?}/04613_data.sql" -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceRegexpOne(raw_blob, '\nSETTINGS version = [0-9]+,', '\nSETTINGS version = 999, future_setting = 1,') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
local_query -q "SELECT engine, position(create_table_query, 'version = 999, future_setting = 1') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'ts_future_version'"

echo '--- every PromQL entry point rejects it ---'
local_query -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query -q "SELECT * FROM prometheusQueryRange(ts_future_version, 'up', 1000, 2000, 60)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query -q "SELECT * FROM timeSeriesSelector(ts_future_version, 'up', 1000, 2000, 'samples')" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query --allow_experimental_time_series_table=1 --dialect=promql --promql_table=ts_future_version -q "up" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1

echo '--- reads, writes, alters and maintenance are rejected too ---'
local_query -q "SELECT * FROM ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query -q "INSERT INTO ts_future_version (metric_name) SELECT 'up'" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query -q "ALTER TABLE ts_future_version MODIFY SETTING filter_by_min_time_and_max_time = false" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query -q "TRUNCATE TABLE ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query -q "SYSTEM CLEAR TIME SERIES CACHES ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
local_query -q "OPTIMIZE TABLE ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1

echo '--- the error is instructive ---'
local_query -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 \
    | grep -o "newer than the latest version" | head -1

rm -rf "${CLICKHOUSE_TMP:?}/04613_data" "${CLICKHOUSE_TMP:?}/04613_data.sql"
