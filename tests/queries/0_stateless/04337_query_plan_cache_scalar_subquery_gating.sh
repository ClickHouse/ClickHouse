#!/usr/bin/env bash
# Tags: no-parallel-replicas
# The query plan cache is incompatible with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_base;
    DROP TABLE IF EXISTS t_other;

    CREATE TABLE t_base (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_other (v Int64) ENGINE = MergeTree ORDER BY v;

    INSERT INTO t_base VALUES (10), (20), (30);
    INSERT INTO t_other VALUES (10), (40);
"

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

# A plain set subquery on the right of IN is cacheable without the opt-in setting: the
# right-hand argument is a set, re-executed on every run, not a scalar subquery.
echo "-- IN with a set subquery on the right is cacheable without the opt-in"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
SET_QUERY="SELECT count() FROM t_base WHERE x IN (SELECT v FROM t_other)"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$SET_QUERY" > /dev/null
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$SET_QUERY" > /dev/null
echo "-- hits: $(hits_of_last_run 'SELECT count() FROM t_base WHERE x IN')"

# A scalar subquery in the LEFT operand of IN is folded into a constant during analysis,
# so it must be gated by query_plan_cache_allow_scalar_subqueries even though it sits next
# to a set subquery. Without the opt-in the query must not be cached.
echo "-- scalar subquery in the LEFT operand of IN is gated (not cached without the opt-in)"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
LEFT_QUERY="SELECT (SELECT max(x) FROM t_base) IN (SELECT v FROM t_other) AS r FROM t_base"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$LEFT_QUERY" > /dev/null
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$LEFT_QUERY" > /dev/null
echo "-- hits: $(hits_of_last_run 'SELECT (SELECT max(x) FROM t_base) IN')"

# With the opt-in, the same query is cacheable (the scalar value is baked into the plan).
echo "-- with the opt-in, the same query is cacheable"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1 --query "$LEFT_QUERY" > /dev/null
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1 --query "$LEFT_QUERY" > /dev/null
echo "-- hits: $(hits_of_last_run 'SELECT (SELECT max(x) FROM t_base) IN')"

# query_plan_cache_allow_scalar_subqueries is part of the cache key: an entry stored with
# the setting enabled must not be reused by a later execution that explicitly disabled it,
# which would otherwise reuse a stale baked scalar value.
echo "-- the opt-in setting is part of the cache key: =1 entry is not reused under =0"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
SCALAR_QUERY="SELECT (SELECT max(x) FROM t_base) AS m FROM t_base"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1 --query "$SCALAR_QUERY" > /dev/null
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1 --query "$SCALAR_QUERY" > /dev/null
echo "-- hits with opt-in (warm): $(hits_of_last_run 'SELECT (SELECT max(x) FROM t_base) AS m')"
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=0 --query "$SCALAR_QUERY" > /dev/null
echo "-- hits without opt-in (must be 0): $(hits_of_last_run 'SELECT (SELECT max(x) FROM t_base) AS m')"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_base;
    DROP TABLE t_other;
"
