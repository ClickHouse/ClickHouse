#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - PromQL requires ANTLR4 support which is disabled in the fast-test build.
# - The experimental TimeSeries table engine does not round-trip through DatabaseReplicated.
#
# A TimeSeries table with a version newer than the latest version known to the server can appear
# after a downgrade of ClickHouse. Such a table can still be attached (so that it can be inspected,
# migrated or dropped), but the PromQL layer must reject queries over it, and the engine must reject
# writes and alters, with instructive errors (see TimeSeriesVersion.h).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
uuid2=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

echo '--- a table with a future version can be attached ---'
# ATTACH TABLE with a full table definition emits a warning which would pollute stderr.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "ATTACH TABLE ts_future_version UUID '$uuid' ENGINE = TimeSeries SETTINGS version = 999"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'ts_future_version'"

echo '--- every PromQL entry point rejects it ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQueryRange(ts_future_version, 'up', 1000, 2000, 60)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM timeSeriesSelector(ts_future_version, 'up', 1000, 2000)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --dialect=promql --promql_table=ts_future_version -q "up" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1

echo '--- writes and alters are rejected too ---'
$CLICKHOUSE_CLIENT -q "INSERT INTO ts_future_version (metric_name) SELECT 'up'" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "ALTER TABLE ts_future_version MODIFY SETTING filter_by_min_time_and_max_time = false" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1

echo '--- the error is instructive ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 \
    | grep -o "newer than the latest version" | head -1

echo '--- a table with a version older than the PromQL layer supports is rejected with a migration hint ---'
# Version 0 never existed, but ATTACH accepts any version, which makes the "too old" branch testable.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "ATTACH TABLE ts_old_version UUID '$uuid2' ENGINE = TimeSeries SETTINGS version = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_old_version, 'up', 1000)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_old_version, 'up', 1000)" 2>&1 \
    | grep -o "copy the data with an INSERT-SELECT query" | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ts_future_version"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts_old_version"
