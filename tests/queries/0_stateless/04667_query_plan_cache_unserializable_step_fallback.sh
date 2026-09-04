#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: a query that is eligible for the query plan cache but contains a step that does not
# support serialization (here `ORDER BY ... WITH FILL`; window functions, the original reproducer,
# have become serializable since) must be executed by the ordinary interpreter, not by the
# cacheable logical-plan path. The logical plan is deliberately built with ordinary planner behaviors
# switched off, so executing it for a query that can never produce a cache entry would make that
# query permanently slower whenever `enable_query_plan_cache` is on, with nothing gained.
# The observable used here is the key-value lookup join: a `Join` engine on the right side of a join
# is read through a direct lookup by the ordinary planner (only the matching keys are read), while a
# cacheable logical plan must not bind that live storage into the plan and reads the whole table.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full rationale
# of the tags).

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

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1" 2>&1
}

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

echo "-- 1. WITH FILL makes the plan unserializable: the query runs through the ordinary interpreter"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
FILL_QUERY="SELECT k, v FROM ${CLICKHOUSE_DATABASE}.t_left ANY LEFT JOIN ${CLICKHOUSE_DATABASE}.t_join USING (k) ORDER BY k WITH FILL FROM 1 TO 6"
echo "-- result: $(run "$FILL_QUERY" | tr '\t' ' ')"
# Three rows of `t_left` looked up in `t_join` by key. Executing the cacheable logical plan instead
# would read all 100000 rows of `t_join`, because a key-value lookup join is not used there.
echo "-- hits and read_rows (must be 0 3 - not cached, and the direct lookup is used): $(stats_of_last_run 'SELECT k, v FROM')"

echo "-- 2. the same query with the plan cache disabled behaves identically"
$CLICKHOUSE_CLIENT --query "$FILL_QUERY" > /dev/null
echo "-- hits and read_rows: $(stats_of_last_run 'SELECT k, v FROM')"

echo "-- 3. a serializable query over the same left table is still cached"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
PLAIN_QUERY="SELECT sum(k) FROM ${CLICKHOUSE_DATABASE}.t_left"
run "$PLAIN_QUERY" > /dev/null
run "$PLAIN_QUERY" > /dev/null
echo "-- hits and read_rows of the second run (must be 1 3, cached): $(stats_of_last_run 'SELECT sum(k) FROM')"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_left;
    DROP TABLE t_join;
"
