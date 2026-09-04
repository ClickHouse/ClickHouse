#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Regression test for the storage-identity race on the query plan cache miss path: a cacheable logical
# plan bakes in the semantics of the storages that were analyzed (here, a row policy that becomes an
# explicit `FilterStep`), while the dependencies of the cache entry and the reads of the plan are
# resolved by name again afterwards. In an `Atomic` database a concurrent `DROP`/`CREATE` makes the
# same name point at a different table in between, so without binding both to the analyzed identities
# the miss would store an entry fingerprinting the new table and execute the new table with the old
# table's row policy - here, returning 1 instead of 6.
# The `query_plan_cache_pause_after_logical_plan` failpoint makes the otherwise narrow window
# deterministic: the plan is built while the old table (and its row policy) exist, and the swap
# happens before dependency collection and execution.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, the
# failpoint is server-wide, and the row policy applies to all users, so the test runs in isolation
# (see 04489 for the full rationale of the tags). The swap relies on an `Atomic` database freeing the
# table name while the paused query still holds the old table, so `Ordinary` and `Replicated`
# databases are excluded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

policy="policy_04655_${CLICKHOUSE_DATABASE}"

# The failpoint is server-wide: if this test ever leaves it enabled, every other query that builds a
# cacheable logical plan on the same server pauses forever. Disable it on every exit path.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1), (2), (3);
    CREATE ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.t USING a = 1 TO ALL;
    SYSTEM DROP QUERY PLAN CACHE;
"

$CLICKHOUSE_CLIENT --query "SELECT 'with the row policy:', sum(a) FROM t"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

# The plan for this query is built while the row policy is in force, then the query blocks in the
# failpoint until the table has been replaced below.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the table was replaced:', sum(a) FROM t" > "${CLICKHOUSE_TMP}/04655_result.txt" 2>&1 &
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
    DROP ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.t;
    DROP TABLE t;
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1), (2), (3);
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

wait $select_pid
cat "${CLICKHOUSE_TMP}/04655_result.txt"
rm -f "${CLICKHOUSE_TMP}/04655_result.txt"

# Re-running the *identical* query probes the cache under the same key: had the plan built against
# the replaced table been stored, this would be a hit that executes the new table with the old
# table's baked row policy, returning 1. It must be a miss that plans the current table from scratch.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the table was replaced:', sum(a) FROM t"

# A query with a different text has its own cache key, so it must be a miss as well.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'the next execution:', sum(a) FROM t"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and misses:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'the next execution%'
    ORDER BY event_time_microseconds DESC LIMIT 1"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and misses of the rerun:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'after the table was replaced%'
    ORDER BY event_time_microseconds DESC LIMIT 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
