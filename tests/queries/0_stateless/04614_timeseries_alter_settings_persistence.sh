#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - Kept aligned with the other TimeSeries tests: the experimental TimeSeries table engine
#   does not round-trip through DatabaseReplicated.
#
# `ALTER TABLE ... MODIFY/RESET SETTING` on a TimeSeries table rebuilds the stored SETTINGS clause
# from the table metadata. This test checks that the rebuild does not lose unrelated settings and
# that a reset which removes the last override is still persisted in the stored table definition
# (by pinning the `version` setting).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

table_def_query="SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'ts_alter_persistence'"

echo '--- a table created before versioning was introduced (no version in the stored definition) ---'
# ATTACH TABLE with a full table definition emits a warning which would pollute stderr.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "ATTACH TABLE ts_alter_persistence UUID '$uuid' ENGINE = TimeSeries SETTINGS filter_by_min_time_and_max_time = false"
$CLICKHOUSE_CLIENT -q "SELECT position(create_table_query, 'version') > 0, position(create_table_query, 'filter_by_min_time_and_max_time') > 0 FROM ($table_def_query)"

echo '--- RESET SETTING which removes the last override is persisted (the version gets pinned instead) ---'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ts_alter_persistence RESET SETTING filter_by_min_time_and_max_time"
$CLICKHOUSE_CLIENT -q "SELECT extract(create_table_query, 'version = (\\d+)'), position(create_table_query, 'filter_by_min_time_and_max_time') > 0 FROM ($table_def_query)"

echo '--- the reset survives DETACH/ATTACH ---'
$CLICKHOUSE_CLIENT -q "DETACH TABLE ts_alter_persistence"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ts_alter_persistence"
$CLICKHOUSE_CLIENT -q "SELECT extract(create_table_query, 'version = (\\d+)'), position(create_table_query, 'filter_by_min_time_and_max_time') > 0 FROM ($table_def_query)"

$CLICKHOUSE_CLIENT -q "DROP TABLE ts_alter_persistence"
