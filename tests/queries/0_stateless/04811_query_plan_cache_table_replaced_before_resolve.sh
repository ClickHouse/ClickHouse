#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Regression test for a cache-accounting leak on the query plan cache miss path: the cache entry must
# be stored only after `resolveStorages` has proven that the plan still binds to the storages it was
# analyzed from. If the entry were stored first and a concurrent `DROP`/`CREATE` replaced a table
# before resolution, the query would correctly fall back to the normal interpreter, but the
# just-inserted entry - already known to be dead - would stay resident, consuming cache size and the
# user's quota until a later execution happens to validation-miss and evict it.
# The `query_plan_cache_pause_before_resolve_storages` failpoint makes the otherwise narrow window
# deterministic: the entry is fully built while the old table exists, and the swap happens before the
# plan's reads are resolved. See 04655 for the companion test of the window before dependency
# collection and for the rationale of the tags (server-wide cache and failpoint, `Atomic`-only swap).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failpoint is server-wide: if this test ever leaves it enabled, every other query that builds a
# cacheable logical plan on the same server pauses forever. Disable it on every exit path.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_before_resolve_storages" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1), (2), (3);
    SYSTEM DROP QUERY PLAN CACHE;
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_before_resolve_storages"

# The cache entry for this query is built while the old table exists, then the query blocks in the
# failpoint until the table has been replaced below.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the table was replaced:', sum(a) FROM t" > "${CLICKHOUSE_TMP}/04811_result.txt" 2>&1 &
select_pid=$!

# Wait until the query is actually paused inside the plan cache miss path. The pattern anchors at the
# start of the query so that this polling query does not match itself.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'after the table was replaced%'") -gt 0 ]] && break
    sleep 0.1
done

# The paused query still holds the old table, so the drop must not wait for the table to be removed
# for real - `database_atomic_wait_for_drop_and_detach_synchronously` is enabled in the test
# configuration and would deadlock with the query this test is pausing on purpose. An `Atomic`
# database frees the name as soon as the table is detached, which is exactly the race being tested.
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 --query "
    DROP TABLE t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (10), (20), (30);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_before_resolve_storages"

wait $select_pid
cat "${CLICKHOUSE_TMP}/04811_result.txt"
rm -f "${CLICKHOUSE_TMP}/04811_result.txt"

# The plan was proven dead before it could be stored (its read binds to the dropped table's UUID),
# so nothing may be left resident in the cache: a dead entry would consume cache size and the user's
# quota with no way to ever produce a hit.
$CLICKHOUSE_CLIENT --query "SELECT 'entries after the race:', value FROM system.metrics WHERE metric = 'QueryPlanCacheEntries'"

# Re-running the identical query probes the cache under the same key: it must be a miss that plans
# the current table from scratch (and this time stores a live entry).
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the table was replaced:', sum(a) FROM t"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and misses of the rerun:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'after the table was replaced%'
    ORDER BY event_time_microseconds DESC LIMIT 1"
$CLICKHOUSE_CLIENT --query "SELECT 'entries after the rerun:', value FROM system.metrics WHERE metric = 'QueryPlanCacheEntries'"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP QUERY PLAN CACHE;
    DROP TABLE t;
"
