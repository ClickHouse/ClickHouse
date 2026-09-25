#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# no-shared-merge-tree: in ClickHouse Cloud `Replicated` databases are managed by the shared catalog
# and the replica state of their clusters is reported differently.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `system.clusters` reads the replica state of `Replicated` databases (`is_active`, `replication_lag`, ...)
# from Keeper. Check that a filter on `cluster` is applied before going to Keeper, that the selected
# databases are read with one request each, and that nothing is read when no replica-state column is
# requested. The number of requests is taken from `system.query_log`; the exact values assume
# ClickHouse Keeper, where the reads of one database are one `ZooKeeperMultiRead` request.

DB1="rdb1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB2="rdb2_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/test/05244/${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup() {
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB1} SYNC" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB2} SYNC" 2>/dev/null || true
    $CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB1} ENGINE = Replicated('${ZK_PATH}/1', 'shard1', 'replica1')"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB2} ENGINE = Replicated('${ZK_PATH}/2', 'shard1', 'replica1')"

# Make sure the clusters of both databases are already built and cached, so that the queries below
# only read the replica state.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.clusters WHERE cluster IN ('${DB1}', '${DB2}')" > /dev/null

run_query() {
    local comment=$1
    local query=$2
    $CLICKHOUSE_CLIENT --log_comment "${comment}" --query "${query}"
}

echo "-- one database selected by cluster"
run_query "05244_one_db" "SELECT cluster = '${DB1}', is_active FROM system.clusters WHERE cluster = '${DB1}'"

echo "-- one database selected by the alias name"
run_query "05244_one_db_alias" "SELECT cluster = '${DB1}', is_active FROM system.clusters WHERE name = '${DB1}'"

echo "-- one database selected via a subquery"
run_query "05244_subquery" "SELECT cluster = '${DB1}', is_active FROM system.clusters WHERE cluster IN (SELECT name FROM system.databases WHERE name = '${DB1}')"

echo "-- two databases selected, one request each"
run_query "05244_two_dbs" "SELECT cluster = '${DB1}', cluster = '${DB2}', is_active FROM system.clusters WHERE cluster IN ('${DB1}', '${DB2}') ORDER BY cluster"

echo "-- no replica-state columns requested"
run_query "05244_no_zk" "SELECT cluster = '${DB1}', host_name != '' FROM system.clusters WHERE cluster = '${DB1}'"

echo "-- filter that cannot be pushed down still gives correct rows"
run_query "05244_not_pushed" "SELECT cluster = '${DB1}' FROM system.clusters WHERE cluster = '${DB1}' OR is_active = 42"

echo "-- all clusters"
run_query "05244_all" "SELECT count() >= 2 FROM (SELECT is_active FROM system.clusters)"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

echo "-- Keeper read requests per query"
$CLICKHOUSE_CLIENT --query "
    SELECT
        log_comment,
        /* other tests may add Replicated databases on another [Zoo]Keeper: one more request per Keeper */
        if(log_comment IN ('05244_all', '05244_not_pushed'), ProfileEvents['ZooKeeperMultiRead'] >= 1, ProfileEvents['ZooKeeperMultiRead'])
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND log_comment IN ('05244_one_db', '05244_one_db_alias', '05244_subquery', '05244_two_dbs', '05244_no_zk', '05244_not_pushed', '05244_all')
        AND event_date >= yesterday()
    ORDER BY log_comment"

echo "-- a database whose Keeper state is broken does not affect the others requested with it"
# The failure is expected: it must not be forwarded to the client at the default send_logs_level
# (any stderr fails the test), see 04278_database_replicated_system_clusters_replicas_info.
$CLICKHOUSE_KEEPER_CLIENT --query "rmr ${ZK_PATH}/1/max_log_ptr"
$CLICKHOUSE_CLIENT --query "SELECT cluster = '${DB1}', cluster = '${DB2}', is_active FROM system.clusters WHERE cluster IN ('${DB1}', '${DB2}') ORDER BY cluster"
