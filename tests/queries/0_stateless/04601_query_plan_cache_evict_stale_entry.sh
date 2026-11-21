#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# A cached plan that fails validation must be evicted from the cache, not merely skipped.
# When the query became permanently uncacheable after being cached (here: `ALTER ... MODIFY
# SQL SECURITY NONE` on a previously-`INVOKER` view), re-planning never stores a replacement,
# so without eviction the dead entry would stay resident forever - still counted in
# `QueryPlanCacheEntries` / `QueryPlanCacheBytes` and in the per-user quota, and re-paying
# validation on every execution.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE and
# exact metric values, so this test runs in isolation (see 04489 for the tag rationale).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_evict;
    DROP VIEW IF EXISTS v_evict;
    CREATE TABLE t_evict (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_evict VALUES (1, 10), (2, 20);
    CREATE VIEW v_evict AS SELECT id, x FROM t_evict;
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1"
}

entries()
{
    $CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'QueryPlanCacheEntries'"
}

bytes_zero()
{
    $CLICKHOUSE_CLIENT --query "SELECT value = 0 FROM system.metrics WHERE metric = 'QueryPlanCacheBytes'"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. INVOKER view query is cached: one entry"
run "SELECT id, x FROM v_evict ORDER BY id"
echo "-- entries: $(entries)"

echo "-- 2. MODIFY SQL SECURITY NONE: the next run fails validation and evicts the entry"
$CLICKHOUSE_CLIENT --query "ALTER TABLE v_evict MODIFY SQL SECURITY NONE"
run "SELECT id, x FROM v_evict ORDER BY id"
echo "-- entries: $(entries)"
echo "-- bytes are zero: $(bytes_zero)"

echo "-- 3. repeated runs stay uncached"
run "SELECT id, x FROM v_evict ORDER BY id"
echo "-- entries: $(entries)"

echo "-- 4. schema change on a plain table: the stale entry is replaced, not leaked"
run "SELECT count() FROM t_evict"
echo "-- entries: $(entries)"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_evict ADD COLUMN y UInt8"
run "SELECT count() FROM t_evict"
echo "-- entries: $(entries)"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW v_evict;
    DROP TABLE t_evict;
"
