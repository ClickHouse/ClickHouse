#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the plain_merge_task_pause_before_prepare failpoint pauses every non-replicated merge of the
# server process while it is enabled, so this test must not run next to tests that merge.
# no-replicated-database, no-shared-merge-tree: the failpoint and the frozen settings snapshot under test live
# in the non-replicated merge path (MergePlainMergeTreeTask); the replicated path freezes its own snapshot in
# MergeFromLogEntryTask::prepare.
#
# A merge runs with the MergeTree settings snapshot taken when it was SELECTED, not with the live ones read
# when the background task finally starts: its up-front memory reservation is priced against those settings
# (CompactionStatistics::estimateNeededMemoryForMerge reads max_compress_block_size, the projection decisions
# and the vertical-merge rules from them), so a concurrent ALTER ... MODIFY SETTING must not change what the
# merge does after the admission gate accepted it at the old price. MergeTask used to re-read
# data->getSettings() in its constructor, which runs only once the queued merge starts.
#
# This test holds a selected merge on the plain_merge_task_pause_before_prepare failpoint, flips
# materialize_projections_on_merge from 1 to 0 while it waits, and then lets it run: the merge must still
# rebuild the projection its reservation priced (MergeTask::prepareProjectionsToMergeAndRebuild reads exactly
# that setting). With the live settings it takes the "projection is not merged" branch instead and the merged
# part ends up with no projection at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare" ||:
}
trap cleanup EXIT

# max_bytes_to_merge_at_max_space_in_pool = 1 keeps background selection away from the parts (OPTIMIZE FINAL
# ignores it), so the only merge ever selected is the one this test issues.
# The projection is added AFTER the inserts, so no source part has it materialized: every part has the same
# (empty) projection set and they are mergeable, while the merge sees "some parts do not have it" and the
# rebuild is decided by materialize_projections_on_merge alone.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_merge_settings_frozen;
    CREATE TABLE t_merge_settings_frozen (k UInt64, payload String) ENGINE = MergeTree ORDER BY k
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1,
                 materialize_projections_on_insert = 0,
                 materialize_projections_on_merge = 1;
    INSERT INTO t_merge_settings_frozen SELECT number, repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_merge_settings_frozen SELECT number, repeat('x', 100) FROM numbers(1000, 1000);
    INSERT INTO t_merge_settings_frozen SELECT number, repeat('x', 100) FROM numbers(2000, 1000);
    ALTER TABLE t_merge_settings_frozen ADD PROJECTION p_frozen (SELECT k, payload ORDER BY payload);
"

# No source part has the projection materialized.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.projection_parts
    WHERE database = currentDatabase() AND table = 't_merge_settings_frozen' AND active"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare"

# The merge is selected here (freezing its settings snapshot, with materialize_projections_on_merge = 1), then
# parks on the failpoint before it starts executing.
$CLICKHOUSE_CLIENT --optimize_throw_if_noop 1 -q "OPTIMIZE TABLE t_merge_settings_frozen FINAL" &
optimize_pid=$!

# Wait until the merge is selected and paused (it is already registered in system.merges by then).
selected=0
for _ in {1..600}
do
    selected=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_settings_frozen'")
    [ "$selected" -ge 1 ] && break
    sleep 0.1
done
if [ "$selected" -lt 1 ]
then
    echo "The merge was not selected in time"
fi

# Change the setting the merge's projection decision depends on, while the selected merge waits.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_merge_settings_frozen MODIFY SETTING materialize_projections_on_merge = 0"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare"
wait $optimize_pid

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_merge_settings_frozen"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_merge_settings_frozen' AND active"
# The merge ran with its selection-time settings snapshot, which still had
# materialize_projections_on_merge = 1: the projection was rebuilt, exactly as the reservation priced it.
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.projection_parts
    WHERE database = currentDatabase() AND table = 't_merge_settings_frozen' AND active"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_merge_settings_frozen"
