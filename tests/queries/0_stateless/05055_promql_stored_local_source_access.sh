#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# A reader of a table created AS prometheusQuery over a local TimeSeries table must hold
# SELECT on that source before the source is looked up, so a rename or a drop is not revealed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_05055_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
SET allow_experimental_time_series_table = 1;
DROP USER IF EXISTS $user;
CREATE TABLE ts_src ENGINE = TimeSeries;
CREATE TABLE ts_stored AS prometheusQuery(ts_src, 'm', 140);
CREATE USER $user;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.ts_stored TO $user;
"

reader() { $CLICKHOUSE_CLIENT --user "$user" -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.ts_stored SETTINGS enable_time_series_aggregate_functions = 1" 2>&1 | grep -o -m1 'ACCESS_DENIED\|TABLE_UUID_MISMATCH\|UNKNOWN_TABLE\|^[0-9]*$'; }

echo '--- no grant on the source'
reader
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${CLICKHOUSE_DATABASE}.ts_src TO ${CLICKHOUSE_DATABASE}.ts_src_renamed"
echo '--- source renamed'
reader
$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}.ts_src_renamed SYNC"
echo '--- source dropped'
reader

echo '--- with the grant'
$CLICKHOUSE_CLIENT -m -q "
SET allow_experimental_time_series_table = 1;
CREATE TABLE ts_src ENGINE = TimeSeries;
DROP TABLE ts_stored;
CREATE TABLE ts_stored AS prometheusQuery(ts_src, 'm', 140);
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user;
"
reader

$CLICKHOUSE_CLIENT -q "DROP USER $user"
