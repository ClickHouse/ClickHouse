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
# The stored definition of a new table is rewritten to a future version and attached back with the short ATTACH.
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE TABLE ts_future_version (metric_name String) ENGINE = TimeSeries"
metadata_file=$($CLICKHOUSE_CLIENT -q "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'ts_future_version'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE ts_future_version"
sed -i.bak 's/^SETTINGS version = [0-9]*,/SETTINGS version = 999, future_setting = 1,/' "$metadata_file" && rm -f "$metadata_file.bak"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ts_future_version"
$CLICKHOUSE_CLIENT -q "SELECT engine, position(create_table_query, 'version = 999, future_setting = 1') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'ts_future_version'"

echo '--- every PromQL entry point rejects it ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQueryRange(ts_future_version, 'up', 1000, 2000, 60)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM timeSeriesSelector(ts_future_version, 'up', 1000, 2000, 'samples')" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --dialect=promql --promql_table=ts_future_version -q "up" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1

echo '--- reads, writes, alters and maintenance are rejected too ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "INSERT INTO ts_future_version (metric_name) SELECT 'up'" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "ALTER TABLE ts_future_version MODIFY SETTING filter_by_min_time_and_max_time = false" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR TIME SERIES CACHES ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ts_future_version" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1

echo '--- the error is instructive ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 \
    | grep -o "newer than the latest version" | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ts_future_version"
