#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test for the per-user quota admission check (`canStoreForUser`). A same-key insertion
# is a replacement: `set` releases the old entry's weight before charging the new one, so admission
# must compare against the size that remains *after* that release. Without this, a user already at
# `query_plan_cache_size_in_bytes_quota` can never refresh an invalidated plan: the stale entry
# keeps its weight counted, the identical-key replacement is rejected, the stale entry stays
# resident, and every later run re-pays validation + replanning without ever updating the cache.
#
# The cache is a single server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE`, the global
# `QueryPlanCacheBytes` metric and exact `QueryPlanCacheHits` counts, and the test creates a global
# user, so it must run in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04496_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_quota;
    CREATE TABLE t_quota (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_quota VALUES (1, 10), (2, 20);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_quota TO $user;
"

QUERY="SELECT a, b FROM ${CLICKHOUSE_DATABASE}.t_quota ORDER BY a"

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE 'SELECT a, b FROM%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

# Measure the weight W of one cached plan for this query. The cache is empty and server-wide, and
# quota=0 (the default) admits unconditionally, so afterwards QueryPlanCacheBytes == W.
$CLICKHOUSE_CLIENT --user="$user" \
    --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query "$QUERY" > /dev/null
W=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'QueryPlanCacheBytes'")

# Pick a quota that fits exactly one such plan but not two: W <= quota < 2*W. With the bug, the
# stale entry (weight W) plus the identical-key replacement (weight ~W) exceed the quota and the
# refresh is rejected; with the fix, the old weight is released first and the replacement fits.
QUOTA=$(( W + W / 2 ))
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_size_in_bytes_quota=$QUOTA"

run_query()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$QUERY"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- warm (miss), fills the user quota"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- second run hits the cached plan"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- ALTER invalidates the cached plan (schema hash changes); the stale entry stays resident"
$CLICKHOUSE_CLIENT --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_quota ADD COLUMN c UInt64 DEFAULT 0"

echo "-- run after ALTER: validation miss, replan, identical-key plan must be re-admitted"
run_query
echo "-- hits: $(hits_of_last_run)"

echo "-- next run must HIT the refreshed plan (0 here means the quota check wrongly rejected the refresh)"
run_query
echo "-- hits: $(hits_of_last_run)"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_quota"
$CLICKHOUSE_CLIENT --query "DROP USER $user"
