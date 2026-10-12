#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# A top-level `UNION ALL` / `UNION DISTINCT` query is cacheable: the second run hits the plan cache,
# a hit still reads current data, and a schema change of either branch's table invalidates the entry.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE and exact
# `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_union_a;
    DROP TABLE IF EXISTS t_union_b;
    CREATE TABLE t_union_a (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_union_b (id UInt64, s String) ENGINE = Memory;
    INSERT INTO t_union_a VALUES (1, 'a1'), (2, 'a2');
    INSERT INTO t_union_b VALUES (2, 'a2'), (3, 'b3');
"

UNION_ALL_QUERY="SELECT id, s FROM t_union_a WHERE id > 0 UNION ALL SELECT id, s FROM t_union_b WHERE id > 0"
UNION_DISTINCT_QUERY="SELECT id, s FROM t_union_a UNION DISTINCT SELECT id, s FROM t_union_b"

run_query()
{
    # The rows of a `UNION` come in an unspecified order, so sort them here.
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1" | sort
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query = {q:String}
        ORDER BY event_time_microseconds DESC
        LIMIT 1" --param_q="$1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- UNION ALL: miss"
run_query "$UNION_ALL_QUERY"
echo "-- hits: $(hits_of_last_run "$UNION_ALL_QUERY")"
echo "-- UNION ALL: hit"
run_query "$UNION_ALL_QUERY"
echo "-- hits: $(hits_of_last_run "$UNION_ALL_QUERY")"

echo "-- UNION DISTINCT: miss"
run_query "$UNION_DISTINCT_QUERY"
echo "-- hits: $(hits_of_last_run "$UNION_DISTINCT_QUERY")"
echo "-- UNION DISTINCT: hit"
run_query "$UNION_DISTINCT_QUERY"
echo "-- hits: $(hits_of_last_run "$UNION_DISTINCT_QUERY")"

echo "-- a hit reads current data of both branches"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_union_a VALUES (4, 'a4')"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_union_b VALUES (5, 'b5')"
run_query "$UNION_ALL_QUERY"
echo "-- hits: $(hits_of_last_run "$UNION_ALL_QUERY")"

echo "-- a schema change of the second branch's table invalidates the entry"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_union_b ADD COLUMN c UInt64 DEFAULT 0"
run_query "$UNION_ALL_QUERY"
echo "-- hits: $(hits_of_last_run "$UNION_ALL_QUERY")"
run_query "$UNION_ALL_QUERY"
echo "-- hits: $(hits_of_last_run "$UNION_ALL_QUERY")"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_union_a;
    DROP TABLE t_union_b;
"
