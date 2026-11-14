#!/usr/bin/env bash
# Tags: shard
# insert_expected_column_types names only columns the user may see: a hidden column is neither confirmed nor typed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_05099_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS d_expected_access;
    DROP TABLE IF EXISTS n_expected_access;
    DROP TABLE IF EXISTS t_expected_access;
    DROP USER IF EXISTS $user;
    CREATE TABLE t_expected_access (x UInt8, secret UUID) ENGINE = Memory;
    CREATE TABLE d_expected_access AS t_expected_access ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, t_expected_access);
    CREATE TABLE n_expected_access (x UInt8) ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, t_expected_access);
    CREATE USER $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.n_expected_access TO $user;
    GRANT INSERT(x) ON ${CLICKHOUSE_DATABASE}.t_expected_access TO $user;
    GRANT INSERT(x) ON ${CLICKHOUSE_DATABASE}.d_expected_access TO $user;
"

run() { $CLICKHOUSE_CLIENT --user "$user" --async_insert 0 -q "$1" 2>&1 | grep -oE 'ACCESS_DENIED|INCOMPATIBLE_SCHEMA' | head -1 | grep . || echo OK; }

echo "hidden column: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'secret': 'String'} VALUES (1)")"
echo "missing column: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'nope': 'String'} VALUES (1)")"
echo "granted column, right type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'x': 'UInt8'} VALUES (2)")"
echo "granted column, wrong type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'x': 'String'} VALUES (3)")"

# The shard of a cluster without a secret checks as the cluster user, so the Distributed table checks the user first.
dist="INSERT INTO d_expected_access (x) SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert"
echo "distributed, hidden column: $(run "$dist = 1, insert_expected_column_types = {'secret': 'String'} VALUES (6)")"
echo "distributed, missing column: $(run "$dist = 1, insert_expected_column_types = {'nope': 'String'} VALUES (6)")"
# A background insert with an expectation is delivered at once, so a refused batch is not left in the queue.
echo "distributed background, wrong type: $(run "$dist = 0, insert_expected_column_types = {'x': 'String'} VALUES (7)")"
echo "distributed background, right type: $(run "$dist = 0, insert_expected_column_types = {'x': 'UInt8'} VALUES (8)")"
# A column only the shard's table has is refused by the narrower Distributed table, not looked up on the shard.
narrow="INSERT INTO n_expected_access (x) SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1"
echo "narrow distributed, shard-only column: $(run "$narrow, insert_expected_column_types = {'secret': 'UUID'} VALUES (9)")"
echo "narrow distributed, declared column: $(run "$narrow, insert_expected_column_types = {'x': 'UInt8'} VALUES (10)")"
$CLICKHOUSE_CLIENT -q "SELECT 'queued files', count() FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'd_expected_access' AND data_files > 0"

$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_expected_access TO $user"
echo "table grant, wrong type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'secret': 'String'} VALUES (4)")"
echo "table grant, right type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'secret': 'UUID'} VALUES (5)")"

$CLICKHOUSE_CLIENT -q "SELECT groupArray(x) FROM (SELECT x FROM t_expected_access ORDER BY x)"
$CLICKHOUSE_CLIENT -q "DROP TABLE d_expected_access; DROP TABLE n_expected_access; DROP TABLE t_expected_access; DROP USER $user"
