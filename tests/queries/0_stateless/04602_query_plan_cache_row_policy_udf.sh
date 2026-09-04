#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: a row policy filter can call a SQL UDF (`CREATE FUNCTION`), which is inlined into the
# filter only when it is actually applied - the raw AST the plan cache fingerprints still shows just
# the call, e.g. `f(x)`. Redefining the UDF with `CREATE OR REPLACE FUNCTION` changes what the
# filter enforces without changing that raw AST, so a hit must not keep enforcing the policy body
# that was live when the plan was cached. Symmetrically, if the UDF body itself contains a
# subquery, that must make the plan uncacheable for the same reason a literal subquery in the
# filter does (see 04495): the read is invisible to the plan leaves and to the cache's dependency
# tracking.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, and the test creates a global user, so it runs in isolation
# (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04602_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_udf;
    DROP TABLE IF EXISTS policy_limits_udf;
    CREATE TABLE t_udf (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE policy_limits_udf (v UInt64) ENGINE = MergeTree ORDER BY v;
    INSERT INTO t_udf VALUES (1), (2), (3), (4), (5);
    INSERT INTO policy_limits_udf VALUES (2);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_udf TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.policy_limits_udf TO $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1
}

# Number of plan cache hits recorded for the most recent run of a query matching $1.
hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE '$1%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

QUERY="SELECT sum(x) FROM ${CLICKHOUSE_DATABASE}.t_udf"

echo "-- 1. row policy calling a subquery-free UDF: query IS cacheable"
$CLICKHOUSE_CLIENT --query "
    CREATE OR REPLACE FUNCTION udf_04602 AS (a) -> a <= 3;
    DROP ROW POLICY IF EXISTS p_udf_04602 ON ${CLICKHOUSE_DATABASE}.t_udf;
    CREATE ROW POLICY p_udf_04602 ON ${CLICKHOUSE_DATABASE}.t_udf USING udf_04602(x) TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
echo "-- boundary <= 3 (rows 1,2,3): $(run_user "$QUERY")"
run_user "$QUERY" > /dev/null
echo "-- hits after a repeat (must be 1, cached): $(hits_of_last_run 'SELECT sum(x) FROM')"

echo "-- 2. redefining the UDF changes the enforced boundary without a stale cache hit"
$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE FUNCTION udf_04602 AS (a) -> a <= 1;"
echo "-- boundary now <= 1 (row 1), reflected immediately: $(run_user "$QUERY")"

echo "-- 3. row policy calling a UDF whose body contains a subquery: query is NOT cacheable"
$CLICKHOUSE_CLIENT --query "
    CREATE OR REPLACE FUNCTION udf_sub_04602 AS (a) -> a <= (SELECT max(v) FROM ${CLICKHOUSE_DATABASE}.policy_limits_udf);
    DROP ROW POLICY IF EXISTS p_udf_04602 ON ${CLICKHOUSE_DATABASE}.t_udf;
    CREATE ROW POLICY p_udf_04602 ON ${CLICKHOUSE_DATABASE}.t_udf USING udf_sub_04602(x) TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
echo "-- boundary = 2 (rows 1,2): $(run_user "$QUERY")"
run_user "$QUERY" > /dev/null
echo "-- hits after a repeat (must be 0, not cached): $(hits_of_last_run 'SELECT sum(x) FROM')"
# Correctness: moving the boundary must take effect immediately - there is no stale cached plan.
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.policy_limits_udf VALUES (4)"
echo "-- boundary now 4 (rows x <= 4), reflected immediately: $(run_user "$QUERY")"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_udf_04602 ON ${CLICKHOUSE_DATABASE}.t_udf;
    DROP USER IF EXISTS $user;
    DROP FUNCTION IF EXISTS udf_04602;
    DROP FUNCTION IF EXISTS udf_sub_04602;
    DROP TABLE t_udf;
    DROP TABLE policy_limits_udf;
"
