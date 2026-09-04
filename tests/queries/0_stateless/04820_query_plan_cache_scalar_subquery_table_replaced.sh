#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Regression test for the storage-identity race on the plan cache miss path for tables read only
# inside a scalar subquery. The scalar is executed during analysis and folded into the plan as a
# constant, so the table it reads has no `ReadFromTable` leaf in the plan - it must still be
# recorded as an analyzed storage identity (see `PlannerJoinTree.cpp`). Without that, a concurrent
# `DROP`/`CREATE` between analysis and dependency collection stores an entry fingerprinting the new
# table while carrying a constant folded from the old one: the entry validates against the new
# table on every later run and keeps serving the dropped table's value, so the documented
# `DROP`/`CREATE` invalidation does not hold. With the identities recorded, the mismatch between
# the fingerprinted UUID and the analyzed UUID is detected, nothing is stored, and the rerun is a
# miss that computes the scalar from the current table.
# The `query_plan_cache_pause_after_logical_plan` failpoint makes the otherwise narrow window
# deterministic: the scalar is folded while the old table exists, and the swap happens before
# dependency collection. See 04655 for the rationale of the tags and of the swap mechanics.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failpoint is server-wide: if this test ever leaves it enabled, every other query that builds a
# cacheable logical plan on the same server pauses forever. Disable it on every exit path.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1);
    SYSTEM DROP QUERY PLAN CACHE;
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

# The scalar subquery is evaluated (over the old table, where max(a) = 1) while the plan is built,
# then the query blocks in the failpoint until the table has been replaced below.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1 \
    --query "SELECT 'after the table was replaced:', (SELECT max(a) FROM t)" > "${CLICKHOUSE_TMP}/04820_result.txt" 2>&1 &
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
# for real (see 04655). The new table holds a different value, so a stale baked scalar is observable.
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 --query "
    DROP TABLE t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (2);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

wait $select_pid
# The result of the raced query itself is not asserted: the query context caches evaluated scalars,
# so even the normal-interpreter fallback legitimately reuses the value computed during its own
# analysis. What must not happen is the stale value being *stored*.
rm -f "${CLICKHOUSE_TMP}/04820_result.txt"

# Nothing may be stored for the raced query: the entry would fingerprint the new table while
# carrying the old table's folded scalar.
$CLICKHOUSE_CLIENT --query "SELECT 'entries after the race:', value FROM system.metrics WHERE name = 'QueryPlanCacheEntries'"

# Re-running the identical query probes the cache under the same key: had the raced entry been
# stored, this would be a hit that keeps returning 1 from the dropped table. It must be a miss that
# evaluates the scalar over the current table and returns 2.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1 \
    --query "SELECT 'after the table was replaced:', (SELECT max(a) FROM t)"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and misses of the rerun:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'after the table was replaced%'
    ORDER BY event_time_microseconds DESC LIMIT 1"
$CLICKHOUSE_CLIENT --query "SELECT 'entries after the rerun:', value FROM system.metrics WHERE name = 'QueryPlanCacheEntries'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
