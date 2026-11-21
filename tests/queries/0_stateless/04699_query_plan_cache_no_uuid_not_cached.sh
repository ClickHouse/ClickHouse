#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# A table without a UUID (a table in an `Ordinary` database) cannot be identity-bound: every guard
# of the "analyzed storage == executed storage" invariant compares storage UUIDs, and after a
# `DROP`/`CREATE` both the old and the new table carry `Nil`, so a swapped table would pass every
# comparison and a stale plan could execute the new table with the old table's baked row policies
# or view expansions. Such queries must not be cached (and must still execute correctly through
# the ordinary interpreter).
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

ORDINARY_DB="${CLICKHOUSE_DATABASE}_ordinary"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 --query "
    DROP DATABASE IF EXISTS ${ORDINARY_DB};
    CREATE DATABASE ${ORDINARY_DB} ENGINE = Ordinary;
" 2>/dev/null

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${ORDINARY_DB}.t (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO ${ORDINARY_DB}.t VALUES (1), (2), (3);
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t VALUES (1), (2), (3);
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1" 2>&1
}

# `QueryPlanCacheHits` of the most recent run of a query matching $1.
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

echo "-- 1. a table in an Ordinary database has no UUID: the plan is never cached"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
ORDINARY_QUERY="SELECT sum(k) FROM ${ORDINARY_DB}.t"
echo "-- result: $(run "$ORDINARY_QUERY")"
echo "-- result: $(run "$ORDINARY_QUERY")"
echo "-- hits of the second run (must be 0, not cached): $(hits_of_last_run 'SELECT sum(k) FROM')"

echo "-- 2. the same query over an Atomic-database table is cached"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
ATOMIC_QUERY="SELECT max(k) FROM ${CLICKHOUSE_DATABASE}.t"
run "$ATOMIC_QUERY" > /dev/null
run "$ATOMIC_QUERY" > /dev/null
echo "-- hits of the second run (must be 1, cached): $(hits_of_last_run 'SELECT max(k) FROM')"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE ${CLICKHOUSE_DATABASE}.t;
    DROP DATABASE ${ORDINARY_DB};
"
