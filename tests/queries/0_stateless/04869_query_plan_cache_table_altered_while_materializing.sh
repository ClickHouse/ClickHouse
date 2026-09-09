#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# Regression test for the semantics race on the plan cache hit path with an in-place `ALTER`.
# `validateQueryPlanCacheEntry` proves that every dependency still has the schema and row policies
# the plan was built with, but the plan's reads are bound to storage snapshots only later, in
# `QueryPlan::resolveStorages`. An `ALTER TABLE ... MODIFY COLUMN` in between keeps the table's
# UUID - so a UUID-only identity check does not fire - while the cached plan still bakes in the
# pre-alter semantics (here: an `ALIAS` column expression). Without the semantics fingerprint
# carried in `QueryPlan::ExpectedStorageIdentities`, the hit would execute the stale plan over the
# post-alter snapshot. With it, resolution rejects the plan (`INCORRECT_DATA`), the hit is counted
# as a stale miss, and the query falls back to normal planning over the current schema.
# The `query_plan_cache_pause_after_validation` failpoint makes the otherwise narrow window
# deterministic. See 04655 for the rationale of the tags.
# The table deliberately uses the `Memory` engine: `MergeTree` metadata reads are pinned per query
# (`enable_shared_storage_snapshot_in_query`), so there resolution sees the validation-time
# metadata and the race cannot occur. `Memory` reads live metadata, which is exactly the drift the
# fingerprint re-check must catch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failpoint is server-wide: if this test ever leaves it enabled, every other plan cache hit on
# the same server pauses forever. Disable it on every exit path.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_validation" 2>/dev/null' EXIT

# The cache key is built from the query text, so all three runs must be byte-identical.
QUERY="SELECT 'the alias column is:', b FROM t"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64, b UInt64 ALIAS a + 1) ENGINE = Memory;
    INSERT INTO t VALUES (1);
    SYSTEM DROP QUERY PLAN CACHE;
"

# Store the entry: the first run is a miss that caches the plan with the pre-alter alias baked in.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT query_plan_cache_pause_after_validation"

# The second run is a hit: the entry validates against the pre-alter schema, then the query blocks
# in the failpoint until the column has been altered below.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --query "$QUERY" > "${CLICKHOUSE_TMP}/04869_result.txt" 2>&1 &
select_pid=$!

# Wait until the query is actually paused inside the plan cache hit path. The pattern anchors at the
# start of the query so that this polling query does not match itself.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.processes
        WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'the alias column is%'") -gt 0 ]] && break
    sleep 0.1
done

# The `ALTER` is metadata-only, so it does not wait for the paused query. The table's UUID stays
# the same - only the semantics fingerprint can detect the change.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t MODIFY COLUMN b UInt64 ALIAS a + 100"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT query_plan_cache_pause_after_validation"

wait $select_pid
# The raced query must return the post-alter value: the fingerprint mismatch in `resolveStorages`
# makes the hit fall back to normal planning, which re-analyzes the current schema.
cat "${CLICKHOUSE_TMP}/04869_result.txt"
rm -f "${CLICKHOUSE_TMP}/04869_result.txt"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
# The raced run must be accounted as a stale miss, not a hit: the validated entry was rejected at
# materialization and evicted, and the fallback re-planning stored a fresh post-alter entry.
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and stale misses of the raced run:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheStaleMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'the alias column is%'
    ORDER BY event_time_microseconds DESC LIMIT 1"

# Re-running the identical query probes the cache under the same key: it must be a genuine hit over
# the fresh entry and keep returning the post-alter value.
$CLICKHOUSE_CLIENT --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'hits and stale misses of the rerun:', ProfileEvents['QueryPlanCacheHits'], ProfileEvents['QueryPlanCacheStaleMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'the alias column is%'
    ORDER BY event_time_microseconds DESC LIMIT 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
