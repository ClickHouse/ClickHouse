#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Pins two eligibility rules of the plan cache documented in `query-plan-cache.mdx`:
#   - a nonzero `allow_experimental_parallel_reading_from_replicas` disables the cache for the
#     whole session, even for a local query that would not otherwise use parallel replicas;
#   - `system.one` is the only `system` table a cacheable query may read; any other `system`
#     table makes the query ineligible.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_05359;
    CREATE TABLE t_05359 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_05359 VALUES (1), (2), (3);
"

# Runs the query $1 twice in separate client sessions and prints the result of the first run.
# Session settings are set with `SET` inside $1 rather than on the command line, because the
# `no-parallel-replicas` tag makes the test runner append `--enable_parallel_replicas 0` after
# all other client options.
run_twice()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1" 2>&1
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1" > /dev/null 2>&1
}

# Plan cache hits recorded for the two runs of a query matching $1, oldest first.
hits_of_runs()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT arrayStringConcat(groupArray(toString(hits)), ' ')
        FROM
        (
            SELECT ProfileEvents['QueryPlanCacheHits'] AS hits
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND type = 'QueryFinish'
              AND query LIKE '$1%'
            ORDER BY event_time_microseconds
        )"
}

echo "-- 1. local query without parallel replicas: the repeat is a hit"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run_twice "SELECT /* q1 */ sum(x) FROM t_05359"
echo "-- hits (must be 0 1): $(hits_of_runs 'SELECT /* q1 */')"

echo "-- 2. same local query with allow_experimental_parallel_reading_from_replicas = 1: both runs miss"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run_twice "SET allow_experimental_parallel_reading_from_replicas = 1; SELECT /* q2 */ sum(x) FROM t_05359"
echo "-- hits (must be 0 0): $(hits_of_runs 'SELECT /* q2 */')"

echo "-- 3. query over system.one: the repeat is a hit"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run_twice "SELECT /* q3 */ count() FROM system.one"
echo "-- hits (must be 0 1): $(hits_of_runs 'SELECT /* q3 */')"

echo "-- 4. query over another system table (system.numbers): both runs miss"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
run_twice "SELECT /* q4 */ sum(number) FROM (SELECT number FROM system.numbers LIMIT 3)"
echo "-- hits (must be 0 0): $(hits_of_runs 'SELECT /* q4 */')"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_05359"
