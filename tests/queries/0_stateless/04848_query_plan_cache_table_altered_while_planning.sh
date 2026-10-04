#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Regression test for the semantics race on the plan cache miss path with an in-place `ALTER`.
# The analyzed plan bakes in the storage's semantics (here: an `ALIAS` column expression), while
# the dependency fingerprints are collected only after the whole plan is built. An
# `ALTER TABLE ... MODIFY COLUMN` in between keeps the table's UUID - so the plain identity check
# does not fire - but makes the dependency record the post-alter schema fingerprint while the plan
# carries the pre-alter expression. Such an entry would validate successfully on every later run
# and keep serving the pre-alter alias value. The semantics fingerprint recorded at analysis time
# (see `Context::PlanCacheStorageIdentities` and `computeQueryPlanCacheSemanticsFingerprint`)
# detects the mismatch: nothing is stored, the raced query falls back to the normal interpreter
# (returning the post-alter value), and the rerun is a miss over the current schema.
# The `query_plan_cache_pause_after_logical_plan` failpoint makes the otherwise narrow window
# deterministic: the plan is built while the old alias exists, and the `ALTER` lands before
# dependency collection. See 04655 for the rationale of the tags.
# The table deliberately uses the `Memory` engine: `MergeTree` metadata reads are pinned per query
# (`enable_shared_storage_snapshot_in_query`), so there dependency collection sees the analysis-time
# metadata and the raced entry merely goes stale (it validation-misses on the next run). `Memory`
# reads live metadata, which is exactly the poisoning case the fingerprint must catch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failpoint is server-wide: if this test ever leaves it enabled, every other query that builds a
# cacheable logical plan on the same server pauses forever. Disable it on every exit path.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64, b UInt64 ALIAS a + 1) ENGINE = Memory;
    INSERT INTO t VALUES (1);
    SYSTEM DROP QUERY PLAN CACHE;
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

# The alias is expanded into the plan (as a + 1, over the old schema) while the plan is built, then
# the query blocks in the failpoint until the column has been altered below.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the alias was altered:', b FROM t" > "${CLICKHOUSE_TMP}/04848_result.txt" 2>&1 &
select_pid=$!

# Wait until the query is actually paused inside the plan cache miss path. The pattern anchors at the
# start of the query so that this polling query does not match itself.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'after the alias was altered%'") -gt 0 ]] && break
    sleep 0.1
done

# The `ALTER` is metadata-only, so it does not wait for the paused query. The table's UUID stays
# the same - only the semantics fingerprint can detect the change.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t MODIFY COLUMN b UInt64 ALIAS a + 100"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_logical_plan"

wait $select_pid
# The raced query itself must return the post-alter value: the fingerprint mismatch makes it fall
# back to the normal interpreter, which re-analyzes the current schema (unlike scalar subqueries,
# alias expressions are not cached in the query context, so this is deterministic).
cat "${CLICKHOUSE_TMP}/04848_result.txt"
rm -f "${CLICKHOUSE_TMP}/04848_result.txt"

# Nothing may be stored for the raced query: the entry would fingerprint the post-alter schema
# while carrying the pre-alter alias expression.
$CLICKHOUSE_CLIENT --query "SELECT 'entries after the race:', value FROM system.metrics WHERE name = 'QueryPlanCacheEntries'"

# Re-running the identical query probes the cache under the same key: had the raced entry been
# stored, this would be a hit that keeps returning the pre-alter value 2. It must be a miss that
# expands the current alias and returns 101.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "SELECT 'after the alias was altered:', b FROM t"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and misses of the rerun:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'after the alias was altered%'
    ORDER BY event_time_microseconds DESC LIMIT 1"
$CLICKHOUSE_CLIENT --query "SELECT 'entries after the rerun:', value FROM system.metrics WHERE name = 'QueryPlanCacheEntries'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
