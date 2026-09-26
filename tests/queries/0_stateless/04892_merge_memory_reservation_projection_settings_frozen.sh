#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the plain_merge_task_pause_before_prepare failpoint pauses every non-replicated merge of
# the server process while it is enabled, so this test must not run next to tests that merge.
# no-replicated-database, no-shared-merge-tree: the failpoint lives in the non-replicated merge path
# (MergePlainMergeTreeTask); the replicated path freezes its settings snapshot in MergeFromLogEntryTask.
#
# A merge runs with the MergeTree settings it was SELECTED with, and that includes every decision the
# projection writers make: the up-front merge memory reservation prices a rebuilt projection from the
# part format and the write buffers of that same snapshot
# (CompactionStatistics::estimateNeededMemoryForMerge), so a merge waiting in the background queue must
# not start writing a Wide projection part - with per-substream buffers - because an
# ALTER ... MODIFY SETTING lowered the wide-part thresholds after the admission gate accepted it.
# This test holds a selected merge on the failpoint, lowers min_bytes_for_wide_part /
# min_rows_for_wide_part to zero while it waits, then lets it run: the rebuilt projection part must keep
# the Compact format the merge was priced for.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare" ||:
}
trap cleanup EXIT

# The projection is added AFTER the inserts, so no source part has it and the merge rebuilds it from the
# merged rows (materialize_projections_on_merge). The parts are far too small for the default wide-part
# thresholds, so the temporary projection part is Compact - the format the reservation is priced from.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_projection_settings_frozen;
    CREATE TABLE t_projection_settings_frozen (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS materialize_projections_on_merge = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
            min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
    INSERT INTO t_projection_settings_frozen SELECT number, number FROM numbers(1000);
    INSERT INTO t_projection_settings_frozen SELECT number + 1000, number FROM numbers(1000);
    INSERT INTO t_projection_settings_frozen SELECT number + 2000, number FROM numbers(1000);
    ALTER TABLE t_projection_settings_frozen ADD PROJECTION p_frozen (SELECT v, sum(k) GROUP BY v);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare"

# The merge is selected here - with the current settings - and then parks on the failpoint before it
# starts executing and writing the projection.
$CLICKHOUSE_CLIENT --optimize_throw_if_noop 1 -q "OPTIMIZE TABLE t_projection_settings_frozen FINAL" &
optimize_pid=$!

# Wait until the merge is selected and paused (it is already registered in system.merges by then).
selected=0
for _ in {1..600}
do
    selected=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_projection_settings_frozen'")
    [ "$selected" -ge 1 ] && break
    sleep 0.1
done
if [ "$selected" -lt 1 ]
then
    echo "The merge was not selected in time"
fi

# Every new part would now be Wide - but not this merge's, which was priced as Compact.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_projection_settings_frozen MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare"
wait $optimize_pid

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_projection_settings_frozen"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_projection_settings_frozen' AND active"
# The rebuilt projection part kept the format of the frozen settings snapshot.
$CLICKHOUSE_CLIENT -q "SELECT name, part_type FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_projection_settings_frozen' AND active ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_projection_settings_frozen"
