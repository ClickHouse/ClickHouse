#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: a query-plan-cache entry must not be created for a query over a table whose effective
# SELECT row policy contains a subquery. Such a filter reads another table and bakes its result (a
# scalar boundary, or an `IN` set) into the plan as a constant, but that table never becomes a plan
# leaf or an entry of the AST closure the cache walks, so a hit would keep enforcing a stale boundary
# after the other table changes. It also bypasses `query_plan_cache_allow_scalar_subqueries`, which
# is applied only to the user query AST (left at its default 0 here). A plain row policy without a
# subquery must stay cacheable.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, and the test creates a global user, so it runs in isolation
# (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04495_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_protected;
    DROP TABLE IF EXISTS t_plain;
    DROP TABLE IF EXISTS policy_limits;
    CREATE TABLE t_protected (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_plain (y UInt64) ENGINE = MergeTree ORDER BY y;
    CREATE TABLE policy_limits (v UInt64) ENGINE = MergeTree ORDER BY v;
    INSERT INTO t_protected VALUES (1), (2), (3), (4), (5);
    INSERT INTO t_plain VALUES (1), (2), (3), (4), (5);
    INSERT INTO policy_limits VALUES (2);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_protected TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_plain TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.policy_limits TO $user;
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

echo "-- 1. row policy with a scalar subquery: query is NOT cacheable"
$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_04495 ON ${CLICKHOUSE_DATABASE}.t_protected;
    CREATE ROW POLICY p_04495 ON ${CLICKHOUSE_DATABASE}.t_protected
        USING x <= (SELECT max(v) FROM ${CLICKHOUSE_DATABASE}.policy_limits) TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
SUB_QUERY="SELECT sum(x) FROM ${CLICKHOUSE_DATABASE}.t_protected"
echo "-- boundary = 2 (rows x <= 2): $(run_user "$SUB_QUERY")"
run_user "$SUB_QUERY" > /dev/null
echo "-- hits after a repeat (must be 0, not cached): $(hits_of_last_run 'SELECT sum(x) FROM')"
# Correctness: moving the boundary must take effect immediately - there is no stale cached plan.
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.policy_limits VALUES (4)"
echo "-- boundary now 4 (rows x <= 4), reflected immediately: $(run_user "$SUB_QUERY")"

echo "-- 2. plain row policy without a subquery: query IS cacheable"
$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_04495 ON ${CLICKHOUSE_DATABASE}.t_protected;
    DROP ROW POLICY IF EXISTS p_plain_04495 ON ${CLICKHOUSE_DATABASE}.t_plain;
    CREATE ROW POLICY p_plain_04495 ON ${CLICKHOUSE_DATABASE}.t_plain USING y <= 3 TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
PLAIN_QUERY="SELECT sum(y) FROM ${CLICKHOUSE_DATABASE}.t_plain"
run_user "$PLAIN_QUERY" > /dev/null
run_user "$PLAIN_QUERY" > /dev/null
echo "-- hits after a repeat (must be 1, cached): $(hits_of_last_run 'SELECT sum(y) FROM')"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_plain_04495 ON ${CLICKHOUSE_DATABASE}.t_plain;
    DROP USER IF EXISTS $user;
    DROP TABLE t_protected;
    DROP TABLE t_plain;
    DROP TABLE policy_limits;
"
