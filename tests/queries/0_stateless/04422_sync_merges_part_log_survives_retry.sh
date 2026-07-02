#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: uses a server-wide pause failpoint that parks every merge after commit
# no-fasttest: relies on background merge execution and the part_log

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=merge_pause_after_commit_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# SYSTEM SYNC MERGES waits for the scheduled merge's part_log row after the result part is active.
# The set of scheduled source parts must survive a SYNC MERGES call that times out or is cancelled
# while still waiting for that part_log write: otherwise a first call that drains the set on
# coverage lets a retry return immediately (empty set) and a following FLUSH LOGS part_log can miss
# the row.
#
# merge_pause_after_commit_before_part_log parks the merge in exactly that window (result part
# committed active, part_log not yet queued). A first SYNC MERGES with a short max_execution_time
# reaches coverage and then times out waiting for part_log. A second SYNC MERGES must still wait for
# the part_log write: with the fix the scheduled set survived the timeout, so count = 1; without the
# fix the first call erased the set on coverage and the retry returns early, so count = 0.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS sm_retry SYNC;
    CREATE TABLE sm_retry (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', old_parts_lifetime = 600;
    INSERT INTO sm_retry VALUES (1);
    INSERT INTO sm_retry VALUES (2);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Schedule the merge of the two parts. It commits all_1_2_1 active and then parks in the
# post-commit/pre-part_log window.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_retry PARTS 'all_1_1_0', 'all_2_2_0'"

# Wait until the merge is provably parked at the failpoint (result part active, part_log not written).
# SYSTEM WAIT FAILPOINT ... PAUSE blocks until a thread reaches the failpoint; fail loudly if it does
# not, so the test never passes without exercising the paused window.
if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    echo "FAIL: merge did not reach the pause failpoint"
    exit 1
fi

# First SYNC MERGES: it reaches coverage (the part is active) and then waits for the part_log write,
# which never comes while the failpoint is paused, so it times out. This is the call that used to
# erase the scheduled set on coverage.
timeout 60 $CLICKHOUSE_CLIENT --max_execution_time 3 -q "SYSTEM SYNC MERGES sm_retry" 2>/dev/null && echo "UNEXPECTED: first SYNC MERGES did not time out"

# Release the pause shortly after, in the background, so the parked merge can queue its part_log row.
( sleep 3 && $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" ) &

# Second SYNC MERGES (retry): it must still wait for the part_log write. With the fix the scheduled
# set survived the timed-out first call, so this blocks until the row is queued; without the fix the
# set was already empty and this returns immediately, so the flush below misses the row.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SYNC MERGES sm_retry;
    SYSTEM FLUSH LOGS part_log;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_retry'
      AND event_type = 'MergeParts' AND part_name = 'all_1_2_1'
    SETTINGS enable_parallel_replicas = 0;
"

wait

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT -q "DROP TABLE sm_retry SYNC"
