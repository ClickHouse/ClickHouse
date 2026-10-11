#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-shared-merge-tree
# Tag no-parallel: fills every merge-executor slot and toggles the server-global failpoint `merge_task_projection_stage_pause`
# Tag no-fasttest: failpoints are not available in the fast test build
# Tag no-shared-merge-tree: the path under test is the `MergeTree` background merge assignment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A background TTL merge selected while the merge pool is full is never scheduled; it must give back its TTL merge
# slot and its partition's `merge_with_ttl_timeout` postponement, so the expired row goes as soon as the pool frees.

pool_tasks=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolSize'")
num_partitions=$((pool_tasks + 2))

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_full_pool_blockers SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_full_pool SYNC"

# Merges of the blockers stop at the projection stage, where the failpoint holds them.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_ttl_full_pool_blockers (p UInt16, k UInt64, PROJECTION agg (SELECT p, count() GROUP BY p))
    ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS min_age_to_force_merge_seconds = 1"

# One expired and one live row, so the only merge is a `TTLDelete` rewrite. A single leaked TTL merge slot or a
# leaked postponement keeps it from ever running within the test.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_ttl_full_pool (k UInt64, d DateTime)
    ENGINE = MergeTree ORDER BY k
    TTL d + INTERVAL 1 SECOND
    SETTINGS ttl_only_drop_parts = 0, merge_with_ttl_timeout = 10000, max_number_of_merges_with_ttl_in_pool = 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_full_pool_blockers SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_full_pool SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES t_ttl_full_pool_blockers"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP TTL MERGES t_ttl_full_pool"

$CLICKHOUSE_CLIENT --query "INSERT INTO t_ttl_full_pool_blockers SELECT number, number FROM numbers($num_partitions)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ttl_full_pool_blockers SELECT number, number + 1000 FROM numbers($num_partitions)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ttl_full_pool VALUES (1, now() - INTERVAL 1 DAY), (2, now() + INTERVAL 1 DAY)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES t_ttl_full_pool_blockers"

full=no
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
    tasks=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask'")
    if [[ "$tasks" -ge "$pool_tasks" ]]; then
        full=yes
        break
    fi
    sleep 0.2
done
echo "merge pool full: $full"

# Wait until the table's merge assignment has run (and postponed itself) after TTL merges are allowed again.
$CLICKHOUSE_CLIENT --query "SYSTEM START TTL MERGES t_ttl_full_pool"
selected=no
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
    ran=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.background_schedule_pool
        WHERE database = currentDatabase() AND table = 't_ttl_full_pool' AND log_name = 'BackgroundJobsAssignee:DataProcessing'
            AND delayed AND NOT scheduled AND NOT executing")
    if [[ "$ran" -eq 1 ]]; then
        selected=yes
        break
    fi
    sleep 0.2
done
echo "merge assignment ran while the pool is full: $selected"
$CLICKHOUSE_CLIENT --query "SELECT 'rows while the pool is full', count() FROM t_ttl_full_pool"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"

deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
    [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM t_ttl_full_pool")" -eq 1 ]] && break
    sleep 0.2
done
$CLICKHOUSE_CLIENT --query "SELECT 'rows after the pool frees', count(), min(k) FROM t_ttl_full_pool"
