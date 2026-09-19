#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: the plain_merge_task_pause_before_prepare failpoint pauses every non-replicated merge
# of the server process while it is enabled, so this test must not run next to tests that merge.
# no-replicated-database, no-shared-merge-tree: the failpoint and the frozen selection timestamp under
# test live in the non-replicated merge path (MergePlainMergeTreeTask); the replicated path pins the
# merge timestamp its own way, via the log entry's create_time.
#
# A non-replicated merge runs with the timestamp it was SELECTED at, not with a fresh one taken when the
# background task starts: the up-front merge memory reservation prices the TTL trigger of
# merge_may_reduce_rows against the selection time (CompactionStatistics::estimateNeededMemoryForMerge),
# so a merge that waits in the background queue while a TTL boundary passes must not execute as a
# row-reducing TTL merge that its reservation priced as an ordinary one. This test holds a selected merge
# on the plain_merge_task_pause_before_prepare failpoint while the rows' TTL expires, then lets it run:
# the merge must keep the rows (its clock predates the boundary). A later merge, selected after the
# boundary, must remove them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare" ||:
}
trap cleanup EXIT

# max_bytes_to_merge_at_max_space_in_pool = 1 keeps size-constrained background selection (regular
# merges AND TTL row-delete merges) away from the parts, while OPTIMIZE FINAL ignores it. The one
# selector NOT bound by it is TTLPartDropMergeSelector, which drops a FULLY expired part the moment its
# max TTL passes - so one row of the last part gets a far-future TTL, keeping every part it ends up in
# alive. The only merges ever selected are then the ones this test issues.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_time_of_merge_frozen;
    CREATE TABLE t_time_of_merge_frozen (k UInt64, d DateTime) ENGINE = MergeTree ORDER BY k
        TTL d + INTERVAL 10 SECOND
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
    INSERT INTO t_time_of_merge_frozen SELECT number, now() FROM numbers(1000);
    INSERT INTO t_time_of_merge_frozen SELECT number + 1000, now() FROM numbers(1000);
    INSERT INTO t_time_of_merge_frozen SELECT number + 2000, now() FROM numbers(1000);
    INSERT INTO t_time_of_merge_frozen SELECT 100000, now() + INTERVAL 1 HOUR;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare"

# The merge is selected here (and its timestamp captured), within the TTL window of the rows just
# inserted, then parks on the failpoint before it starts executing.
$CLICKHOUSE_CLIENT --optimize_throw_if_noop 1 -q "OPTIMIZE TABLE t_time_of_merge_frozen FINAL" &
optimize_pid=$!

# Wait until the merge is selected and paused (it is already registered in system.merges by then).
selected=0
for _ in {1..600}
do
    selected=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_time_of_merge_frozen'")
    [ "$selected" -ge 1 ] && break
    sleep 0.1
done
if [ "$selected" -lt 1 ]
then
    echo "The merge was not selected in time"
fi

# Let the TTL boundary of the ordinary rows pass while the selected merge is held on the failpoint.
while [ "$($CLICKHOUSE_CLIENT -q "SELECT max(d) + INTERVAL 11 SECOND <= now() FROM t_time_of_merge_frozen WHERE k < 100000")" != "1" ]
do
    sleep 0.3
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare"
wait $optimize_pid

# The merge ran with its selection-time clock, which predates the TTL boundary: all rows survive.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_time_of_merge_frozen"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_time_of_merge_frozen' AND active"

# A merge selected AFTER the boundary does remove the expired rows: the fresh insert keeps a mergeable
# second part, and only the two far-future rows survive.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_time_of_merge_frozen SELECT 100001, now() + INTERVAL 1 HOUR"
$CLICKHOUSE_CLIENT --optimize_throw_if_noop 1 -q "OPTIMIZE TABLE t_time_of_merge_frozen FINAL"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_time_of_merge_frozen"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_time_of_merge_frozen"
