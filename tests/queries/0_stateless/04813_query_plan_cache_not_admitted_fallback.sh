#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: a query whose plan cache entry is not admitted into the cache (here because
# `query_plan_cache_size_in_bytes_quota` is smaller than the serialized entry) must be executed by
# the ordinary interpreter, not by the cacheable logical-plan path. `QueryPlanCache::set` returns
# without storing on a per-user quota rejection and when the entry is larger than the whole cache,
# so such a query is permanently non-storable: executing the cacheable plan for it (with ordinary
# planner behaviors like direct-join lookups switched off) would make it permanently slower
# whenever `enable_query_plan_cache` is on, with no compensating hit ever.
# The observable is the same as in 04667: a `Join` engine on the right side of a join is read
# through a direct lookup by the ordinary planner (only the matching keys are read), while a
# cacheable logical plan reads the whole table.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_left;
    DROP TABLE IF EXISTS t_join;
    CREATE TABLE t_left (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO t_left VALUES (1), (2), (3);
    INSERT INTO t_join SELECT number, number FROM numbers(100000);
"

# `QueryPlanCacheHits` and `read_rows` of the most recent run of a query matching $1.
stats_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits'], read_rows
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE '$1%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1" | tr '\t' ' '
}

entries()
{
    $CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'QueryPlanCacheEntries'"
}

JOIN_QUERY="SELECT k, v FROM ${CLICKHOUSE_DATABASE}.t_left ANY LEFT JOIN ${CLICKHOUSE_DATABASE}.t_join USING (k) ORDER BY k"

echo "-- 1. a 1-byte quota rejects the entry: the query runs through the ordinary interpreter every time"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query_plan_cache_size_in_bytes_quota=1 --query "$JOIN_QUERY"
# Three rows of `t_left` looked up in `t_join` by key. Executing the cacheable logical plan instead
# would read all 100000 rows of `t_join`, because a key-value lookup join is not used there.
echo "-- hits and read_rows (must be 0 3 - not cached, and the direct lookup is used): $(stats_of_last_run 'SELECT k, v FROM')"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query_plan_cache_size_in_bytes_quota=1 --query "$JOIN_QUERY" > /dev/null
echo "-- hits and read_rows of the second run (must still be 0 3): $(stats_of_last_run 'SELECT k, v FROM')"
echo "-- entries in the cache (must be 0): $(entries)"

echo "-- 2. the same query with no quota is stored and hits"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$JOIN_QUERY" > /dev/null
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$JOIN_QUERY" > /dev/null
echo "-- hits of the second run (must be 1, cached): $(stats_of_last_run 'SELECT k, v FROM' | cut -d' ' -f1)"
echo "-- entries in the cache (must be 1): $(entries)"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_left;
    DROP TABLE t_join;
"
