#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-parallel
# no-shared-merge-tree: SharedMergeTree doesn't load inactive parts to memory after restart
# no-parallel: SYSTEM ENABLE FAILPOINT is process-wide, the pause failpoint would block the
# loading of outdated parts of the tables of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_retryable_error" 2>/dev/null
}
trap cleanup EXIT

# The loading of outdated parts is cancelled by DETACH TABLE while the loading workers are in flight,
# and then the workers fail with a retryable error. The DETACH must not hang: the workers must not
# take the mutex the cancellation branch waits under, and the failed parts must be requeued only
# after all the workers finished.
#
# The number of parts is greater than the size of the loading thread pool plus its queue
# (`max_outdated_parts_loading_thread_pool_size`, 32 each), so the dispatcher still has parts
# to hand out when the cancellation arrives and the cancellation branch is really exercised.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_load_outdated_parts_cancel;
    CREATE TABLE t_load_outdated_parts_cancel (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS old_parts_lifetime = 600;
    SYSTEM STOP MERGES t_load_outdated_parts_cancel;
    INSERT INTO t_load_outdated_parts_cancel SELECT number FROM numbers(100)
        SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1, max_insert_threads = 1;
    SYSTEM START MERGES t_load_outdated_parts_cancel;
    OPTIMIZE TABLE t_load_outdated_parts_cancel FINAL;
"

outdated_before=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_cancel' AND NOT active")
[ "$outdated_before" -ge 100 ] || echo "Unexpected number of outdated parts: $outdated_before"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE t_load_outdated_parts_cancel;
    SYSTEM ENABLE FAILPOINT merge_tree_load_outdated_parts_pause;
    SYSTEM ENABLE FAILPOINT merge_tree_load_outdated_parts_retryable_error;
    ATTACH TABLE t_load_outdated_parts_cancel;
    SYSTEM WAIT FAILPOINT merge_tree_load_outdated_parts_pause PAUSE;
"

# Cancel the loading while the workers are paused. DETACH waits for the loading task to stop.
$CLICKHOUSE_CLIENT --query "DETACH TABLE t_load_outdated_parts_cancel" &
detach_pid=$!

for _ in $(seq 1 300); do
    detach_running=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE current_database = currentDatabase() AND query LIKE 'DETACH TABLE t_load_outdated_parts_cancel%'")
    [ "$detach_running" = "1" ] && break
    sleep 0.1
done
# Give the DETACH a moment to reach the loading task and cancel it.
sleep 0.5

# Let the workers fail with the retryable error.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_pause"

wait $detach_pid
echo "DETACH finished"

# Without the failpoints, all the outdated parts are loaded after ATTACH.
$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_retryable_error;
    ATTACH TABLE t_load_outdated_parts_cancel;
    SYSTEM WAIT LOADING PARTS t_load_outdated_parts_cancel;
"

outdated_after=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_cancel' AND NOT active")
[ "$outdated_after" = "$outdated_before" ] && echo "All outdated parts are loaded" || echo "Outdated parts before: $outdated_before, after: $outdated_after"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_load_outdated_parts_cancel"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_load_outdated_parts_cancel"
