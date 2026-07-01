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

# SYSTEM SYNC MERGES captures the set of scheduled source parts once, at entry, and every wait clause
# (coverage, merge list, fetch) plus the final clearScheduledParts() must be scoped to that snapshot.
# Otherwise a concurrent SYSTEM SCHEDULE MERGE for unrelated parts, issued after the snapshot, keeps
# the first call waiting on parts it never scheduled: the coverage check used to walk the live
# registry instead of the captured snapshot.
#
# p parts all_1_1_0 / all_2_2_0 are scheduled and their merge parks at the failpoint (result part
# all_1_2_1 active, part_log not yet queued), so SYNC MERGES for {p} enters its wait loop via the
# merge list clause. While it waits, a second SCHEDULE MERGE registers unrelated q parts all_3_3_0 /
# all_5_5_0 that can never be merged (all_4_4_0 sits between them, so the Manual selector never
# assembles the range) and stay uncovered forever. When the failpoint is released the p-merge queues
# its part_log row and drops its merge list entry, so the {p} snapshot is fully satisfied. With the
# fix SYNC MERGES returns (SYNC_OK); without it the coverage check still sees the uncovered q parts
# in the live registry and waits until max_execution_time (SYNC_TIMEOUT).
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS sm_snap SYNC;
    CREATE TABLE sm_snap (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', old_parts_lifetime = 600;
    INSERT INTO sm_snap VALUES (1);
    INSERT INTO sm_snap VALUES (2);
    INSERT INTO sm_snap VALUES (3);
    INSERT INTO sm_snap VALUES (4);
    INSERT INTO sm_snap VALUES (5);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Schedule the p-merge. It commits all_1_2_1 active and then parks in the post-commit/pre-part_log
# window.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_snap PARTS 'all_1_1_0', 'all_2_2_0'"

# Prove the p-merge is parked (result part active, part_log not written), so SYNC MERGES stays in its
# wait loop via the merge list clause rather than returning immediately.
if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    echo "FAIL: p-merge did not reach the pause failpoint"
    exit 1
fi

# Start SYNC MERGES for {p1,p2}: it captures the snapshot now (only p is scheduled) and enters its
# wait loop because the parked p-merge still holds its merge list entry.
res_file="${CLICKHOUSE_TMP}/sm_snap_${CLICKHOUSE_DATABASE}.res"
( $CLICKHOUSE_CLIENT --max_execution_time 10 -q "SYSTEM SYNC MERGES sm_snap" 2>/dev/null \
    && echo SYNC_OK > "$res_file" || echo SYNC_TIMEOUT > "$res_file" ) &
sync_pid=$!

# Let SYNC MERGES capture its {p} snapshot and settle into the wait loop.
sleep 3

# Concurrent SCHEDULE MERGE for unrelated q parts, AFTER the snapshot. all_3_3_0 and all_5_5_0 are
# non-adjacent (all_4_4_0 is between them), so the Manual selector can never assemble this range and
# the q parts stay scheduled but uncovered.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_snap PARTS 'all_3_3_0', 'all_5_5_0'"

# Release the p-merge so it queues its part_log row and drops its merge list entry. The {p} snapshot
# is now fully satisfied; only the coverage clause's scoping decides whether SYNC MERGES returns.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

wait "$sync_pid"
cat "$res_file"
rm -f "$res_file"

$CLICKHOUSE_CLIENT -q "DROP TABLE sm_snap SYNC"
