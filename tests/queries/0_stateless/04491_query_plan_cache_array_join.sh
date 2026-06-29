#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a query using `arrayJoin` must be cacheable. `arrayJoin` reports
# `isDeterministic() = false` because it is multi-valued, but it is pure, so the plan cache
# exempts it. Without the exemption in the post-analysis eligibility check, the second run
# below would not hit the cache. The query plan cache is a single, server-wide cache inspected
# via `SYSTEM DROP QUERY PLAN CACHE` and exact `QueryPlanCacheHits`, so this test runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

# A view body that uses `arrayJoin`, plus a top-level query that also uses `arrayJoin`.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_arr;
    DROP VIEW IF EXISTS v_arr;

    CREATE TABLE t_arr (id UInt64, xs Array(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_arr VALUES (1, [10, 20]), (2, [30]);

    CREATE VIEW v_arr AS SELECT id, arrayJoin(xs) AS x FROM t_arr;
"

QUERY="SELECT id, x + arrayJoin([0, 100]) AS v FROM v_arr ORDER BY id, v"

run_query()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE 'SELECT id, x + arrayJoin%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- first run (cold, miss)"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- second run: arrayJoin query must hit the plan cache"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- data freshness: cache hit must see newly inserted rows"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_arr VALUES (3, [40])"
run_query
echo "-- hits: $(hits_of_last_run)"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW v_arr;
    DROP TABLE t_arr;
"
