#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: `max_threads` is not part of the query plan cache key, so a warm-up with
# `SETTINGS max_threads = 1` and a later run with a different value share one cache entry. The
# stored plan carries the thread cap that the planner stamped on it at warm-up, and so does the
# source plan of every `IN (SELECT ...)` set, which is rebuilt on a hit and turned into a pipeline of
# its own. A hit must drop all of these caps and fan out according to the current query, otherwise
# the set-building sub-pipeline stays pinned to a single thread for as long as the entry lives.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, so the
# test runs in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --log_processors_profiles=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS outer_tbl;
    DROP TABLE IF EXISTS inner_tbl;
    CREATE TABLE outer_tbl (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE inner_tbl (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO outer_tbl SELECT number FROM numbers(1000000);
    INSERT INTO inner_tbl SELECT number FROM numbers(1000000);
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1"
}

# Each table is a single part of a million rows, so a read of it is split into exactly `max_threads`
# streams and the number of `MergeTreeSelect` processors in the query equals the fan-out the plan was
# built with: the outer read plus the set source give 8 with `max_threads = 4` on both, and 5 when the
# set source keeps the warm-up cap of 1.
report_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, processors_profile_log"
    $CLICKHOUSE_CLIENT --query "
        WITH (
            SELECT query_id
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND type = 'QueryFinish'
              AND query = '$1'
            ORDER BY event_time_microseconds DESC
            LIMIT 1
        ) AS last_query_id
        SELECT
            'hits: ' || toString((SELECT ProfileEvents['QueryPlanCacheHits'] FROM system.query_log WHERE query_id = last_query_id AND type = 'QueryFinish')),
            'MergeTree read streams: ' || toString(countIf(name LIKE 'MergeTreeSelect%'))
        FROM system.processors_profile_log
        WHERE query_id = last_query_id
        FORMAT TSV"
}

QUERY_TEMPLATE="SELECT count() FROM outer_tbl WHERE k IN (SELECT k FROM inner_tbl WHERE k % 2 = 0)"
WARM_QUERY="$QUERY_TEMPLATE SETTINGS max_threads = 1"
HIT_QUERY="$QUERY_TEMPLATE SETTINGS max_threads = 4"

echo "-- reference: max_threads = 4 on a miss"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run "$HIT_QUERY"
report_last_run "$HIT_QUERY"

echo "-- warm up with max_threads = 1, then hit with max_threads = 4"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run "$WARM_QUERY"
report_last_run "$WARM_QUERY"
run "$HIT_QUERY"
report_last_run "$HIT_QUERY"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE outer_tbl;
    DROP TABLE inner_tbl;
"
