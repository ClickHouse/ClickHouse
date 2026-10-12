#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# A materialized view with an explicit `SQL SECURITY DEFINER` or `SQL SECURITY NONE` executes under an
# overridden security context that a cached plan cannot replay, so `isStorageEligibleForPlanCache`
# refuses to cache a query that reads it: both runs must be misses, while still returning correct
# results. The complementary case of a materialized view without an explicit clause (cacheable) is
# covered by 04651. The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN
# CACHE and exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS mv_definer;
    DROP TABLE IF EXISTS mv_none;
    DROP TABLE IF EXISTS t_target_definer;
    DROP TABLE IF EXISTS t_target_none;
    DROP TABLE IF EXISTS t_src;
    CREATE TABLE t_src (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE t_target_definer (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE t_target_none (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_definer TO t_target_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT a, b FROM t_src;
    CREATE MATERIALIZED VIEW mv_none TO t_target_none SQL SECURITY NONE AS SELECT a, b FROM t_src;
    INSERT INTO t_src VALUES (1, 10), (2, 20);
"

hits_and_misses_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query = {q:String}
        ORDER BY event_time_microseconds DESC
        LIMIT 1" --param_q="$1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

for mv in mv_definer mv_none
do
    QUERY="SELECT a, b FROM $mv ORDER BY a"
    for run in 1 2
    do
        echo "-- $mv, run $run: correct result, never cached"
        # shellcheck disable=SC2086
        $CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"
        echo "-- hits, misses: $(hits_and_misses_of_last_run "$QUERY")"
    done
done

$CLICKHOUSE_CLIENT --query "SELECT '-- entries: ' || toString(value) FROM system.metrics WHERE name = 'QueryPlanCacheEntries'"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE mv_definer;
    DROP TABLE mv_none;
    DROP TABLE t_target_definer;
    DROP TABLE t_target_none;
    DROP TABLE t_src;
"
