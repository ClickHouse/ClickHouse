#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test for query-plan-cache view security handling:
#   1. A query over a `SQL SECURITY NONE` view must fall back to normal execution and never be
#      cached: the cached plan resolves the expanded view leaves under the invoker context, which
#      would change the security context the view body should run under.
#   2. Changing a view from `INVOKER` to `SQL SECURITY NONE` must invalidate a cached entry (the
#      SQL security type is part of the dependency fingerprint and is re-checked on validation),
#      after which the query is no longer cached.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE and
# exact `QueryPlanCacheHits`, so this test runs in isolation (see 04489 for the tag rationale).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_base;
    DROP VIEW IF EXISTS v_none;
    DROP VIEW IF EXISTS v_inv;
    CREATE TABLE t_base (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_base VALUES (1, 10), (2, 20);
    CREATE VIEW v_none SQL SECURITY NONE AS SELECT id, x FROM t_base;
    CREATE VIEW v_inv AS SELECT id, x FROM t_base;
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1"
}

hits_of()
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

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. SQL SECURITY NONE view: correct result, never cached"
run "SELECT id, x FROM v_none ORDER BY id"
echo "-- hits: $(hits_of 'SELECT id, x FROM v_none')"
run "SELECT id, x FROM v_none ORDER BY id"
echo "-- hits (still a miss, not cached): $(hits_of 'SELECT id, x FROM v_none')"

echo "-- 2. INVOKER view: miss then hit"
run "SELECT id, x FROM v_inv ORDER BY id"
echo "-- hits: $(hits_of 'SELECT id, x FROM v_inv')"
run "SELECT id, x FROM v_inv ORDER BY id"
echo "-- hits: $(hits_of 'SELECT id, x FROM v_inv')"

echo "-- 2b. MODIFY SQL SECURITY NONE invalidates the entry; query is no longer cached"
$CLICKHOUSE_CLIENT --query "ALTER TABLE v_inv MODIFY SQL SECURITY NONE"
run "SELECT id, x FROM v_inv ORDER BY id"
echo "-- hits: $(hits_of 'SELECT id, x FROM v_inv')"
run "SELECT id, x FROM v_inv ORDER BY id"
echo "-- hits (still not cached): $(hits_of 'SELECT id, x FROM v_inv')"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW v_none;
    DROP VIEW v_inv;
    DROP TABLE t_base;
"
