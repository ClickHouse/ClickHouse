#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a zero-column read (`SELECT count() FROM mv`) of a materialized view must not be
# cached. The planner injects the currently-smallest *granted* column purely to let the storage
# produce the correct number of rows, and `StorageMaterializedView::readImpl` re-checks exactly that
# column name against the source table of the view's defining `SELECT`. A cached plan would replay
# the column chosen at store time, so once that column is revoked the hit throws `ACCESS_DENIED`
# while a miss re-plans with another granted column and succeeds. The same applies to `Buffer`,
# which re-checks the plan's column names against its destination table.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, and the
# test creates a global user, so it runs in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05054_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

# `a` is strictly smaller than `b`, so while both are granted the injected helper column is `a`.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS mv_zero_col;
    DROP TABLE IF EXISTS t_zero_col_src;
    CREATE TABLE t_zero_col_src (a UInt8, b UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE MATERIALIZED VIEW mv_zero_col (a UInt8, b UInt64) ENGINE = MergeTree ORDER BY tuple()
        AS SELECT a, b FROM t_zero_col_src;
    INSERT INTO t_zero_col_src VALUES (1, 10), (2, 20), (3, 30);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
"

COUNT_QUERY="SELECT count() FROM ${CLICKHOUSE_DATABASE}.mv_zero_col"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND current_database = currentDatabase()
          AND user = '$user'
          AND query LIKE 'SELECT count() FROM%mv_zero_col%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

$CLICKHOUSE_CLIENT --query "
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.mv_zero_col TO $user;
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_zero_col_src TO $user;
"

echo "-- both columns granted; the helper column is a:"
run_user "$COUNT_QUERY"
echo "-- hits: $(hits_of_last_run)"

echo "-- the plan must not be cached, so the second run is a miss too:"
run_user "$COUNT_QUERY"
echo "-- hits: $(hits_of_last_run)"

echo "-- grants moved from the helper column to the other one; re-planning must still succeed:"
$CLICKHOUSE_CLIENT --query "
    REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.mv_zero_col FROM $user;
    REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_zero_col_src FROM $user;
"
run_user "$COUNT_QUERY"
echo "-- hits: $(hits_of_last_run)"

echo "-- with no column granted at all the query must be denied:"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(b) ON ${CLICKHOUSE_DATABASE}.mv_zero_col FROM $user"
run_user "$COUNT_QUERY" | grep -Fo "ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    DROP TABLE mv_zero_col;
    DROP TABLE t_zero_col_src;
"
