#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - PromQL requires ANTLR4 support which is disabled in the fast-test build.
# - The experimental TimeSeries table engine does not round-trip through DatabaseReplicated.
#
# A TimeSeries table with a version newer than the latest version known to the server can appear
# after a downgrade of ClickHouse. Such a table can still be attached (so that it can be inspected,
# migrated or dropped), but the PromQL layer must reject queries over it with an instructive error
# (see TimeSeriesVersion.h).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

echo '--- a table with a future version can be attached ---'
# ATTACH TABLE with a full table definition emits a warning which would pollute stderr.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "ATTACH TABLE ts_future_version UUID '$uuid' ENGINE = TimeSeries SETTINGS version = 999"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'ts_future_version'"

echo '--- every PromQL entry point rejects it ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQueryRange(ts_future_version, 'up', 1000, 2000, 60)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM timeSeriesSelector(ts_future_version, 'up', 1000, 2000)" 2>&1 | grep -o "INCOMPATIBLE_SCHEMA" | head -1

echo '--- the error is instructive ---'
$CLICKHOUSE_CLIENT -q "SELECT * FROM prometheusQuery(ts_future_version, 'up', 1000)" 2>&1 \
    | grep -o "newer than the latest version" | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ts_future_version"
