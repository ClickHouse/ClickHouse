#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test occupies every merge-executor slot and toggles the server-global
#   failpoints `merge_task_projection_stage_pause` and `mt_merge_selecting_task_pause_when_scheduled`.
# - no-fasttest: failpoints are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table-wide `OPTIMIZE TABLE t` selects a merge before it reserves a merge-executor slot. When no
# slot is free, the selection is discarded and retried with a slot in hand. Selecting a `TTLDelete`
# merge also postpones the next TTL merge of the partition by `merge_with_ttl_timeout`, so the
# discard has to give that postponement back. The postponement is shared with background selection,
# so leaking it defers the TTL rewrite of a partially expired single part - which has no regular
# merge to fall back to - for the whole timeout, instead of running it as soon as a slot frees.

pool_size=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'")

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_retry_blockers SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_retry SYNC"

# The blocker table keeps a foreground merge - and therefore an executor slot reservation - in
# flight for every worker. A projection makes each merge stop at the projection stage, where the
# failpoint pauses it, and `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges
# away from its parts.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_ttl_retry_blockers (p UInt16, k UInt64, v UInt64, PROJECTION agg (SELECT p, sum(v) GROUP BY p))
    ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS optimize_on_insert = 0, max_bytes_to_merge_at_max_space_in_pool = 1, min_age_to_force_merge_seconds = 0"

for ((partition = 0; partition < pool_size; ++partition)); do
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_ttl_retry_blockers VALUES ($partition, 1, 1), ($partition, 2, 2)"
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_ttl_retry_blockers VALUES ($partition, 3, 3), ($partition, 4, 4)"
done

# The target table holds a single part with one expired and one live row, so its only possible merge
# is a `TTLDelete` rewrite. `ttl_only_drop_parts = 0` keeps it a rewrite instead of a part drop
# (a `TTLDrop` merge postpones nothing), and the long `merge_with_ttl_timeout` makes a leaked
# postponement fatal for the test instead of merely slow. TTL merges stay stopped until everything
# else is in place, so that nothing can rewrite the part ahead of the `OPTIMIZE` below.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_ttl_retry (k UInt64, d DateTime)
    ENGINE = MergeTree ORDER BY k
    TTL d + INTERVAL 1 SECOND
    SETTINGS optimize_on_insert = 0, ttl_only_drop_parts = 0, merge_with_ttl_timeout = 10000"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP TTL MERGES t_ttl_retry"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ttl_retry VALUES (1, now() - INTERVAL 1 DAY), (2, now() + INTERVAL 1 DAY)"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_retry_blockers SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_retry SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

# Reserve every executor slot with foreground merges.
for ((partition = 0; partition < pool_size; ++partition)); do
    $CLICKHOUSE_CLIENT --receive_timeout 900 --query "OPTIMIZE TABLE t_ttl_retry_blockers PARTITION ID '$partition' FINAL" &
done

deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
    active_blockers=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_ttl_retry_blockers'")
    [[ "$active_blockers" -eq "$pool_size" ]] && break
    sleep 0.2
done
echo "all merge slots reserved: $([[ "$active_blockers" -eq "$pool_size" ]] && echo yes || echo no)"

# Park background merge selection, so that the TTL rewrite below can only be performed by the
# foreground `OPTIMIZE` - a background selection would hide a leaked postponement by doing it once
# the slots free. Foreground `OPTIMIZE` does not go through `scheduleDataProcessingJob`.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE"

$CLICKHOUSE_CLIENT --query "SYSTEM START TTL MERGES t_ttl_retry"
$CLICKHOUSE_CLIENT --receive_timeout 900 --query_id "ttl_retry_$CLICKHOUSE_DATABASE" --query "OPTIMIZE TABLE t_ttl_retry" &
optimize_pid=$!

# Wait until that `OPTIMIZE` has been running long enough to have selected the TTL merge and given
# it up again: no slot can free until the failpoint below is disabled, so it is waiting for one.
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
    # If the query is already gone, there is nothing left to wait for.
    kill -0 "$optimize_pid" 2>/dev/null || break
    elapsed=$($CLICKHOUSE_CLIENT --query "SELECT max(elapsed) FROM system.processes WHERE query_id = 'ttl_retry_$CLICKHOUSE_DATABASE'")
    [[ "${elapsed%%.*}" -ge 2 ]] && break
    sleep 0.2
done

# Free the slots. The waiting `OPTIMIZE` must select the TTL merge again and drop the expired row.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait "$optimize_pid"

$CLICKHOUSE_CLIENT --query "SELECT 'rows after optimize', count(), min(k) FROM t_ttl_retry"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled"
wait
