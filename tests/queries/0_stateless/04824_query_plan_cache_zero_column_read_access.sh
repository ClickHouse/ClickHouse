#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a query-plan-cache hit for a zero-column read (`SELECT count() FROM t`) must
# re-check access with the same "SELECT on at least one column" rule as planning. The planner
# injects the currently-smallest *granted* column purely to let the storage produce the correct
# number of rows; that helper column is not the query's access contract, so it must not be recorded
# as a required column of the cached plan's dependency. Otherwise, with grants moved from the helper
# column to another column between the miss and the hit, a re-plan succeeds by choosing the other
# column while the hit throws `ACCESS_DENIED` on the stale helper column.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, and the
# test creates a global user, so it runs in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04824_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

# `a` is strictly smaller than `b`, so while both are granted the injected helper column is `a`.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_zero_col;
    CREATE TABLE t_zero_col (a UInt8, b UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_zero_col VALUES (1, 10), (2, 20), (3, 30);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
"

COUNT_QUERY="SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_zero_col"

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
          AND user = '$user'
          AND query LIKE 'SELECT count() FROM%t_zero_col%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

$CLICKHOUSE_CLIENT --query "GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_zero_col TO $user"

echo "-- miss (both columns granted; the helper column is a):"
run_user "$COUNT_QUERY"
echo "-- hits: $(hits_of_last_run)"

echo "-- grants moved from the helper column to the other one; the hit must still be allowed:"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_zero_col FROM $user"
run_user "$COUNT_QUERY"
echo "-- hits: $(hits_of_last_run)"

echo "-- with no column granted at all the hit must be denied:"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_zero_col FROM $user"
run_user "$COUNT_QUERY" | grep -Fo "ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    DROP TABLE t_zero_col;
"
