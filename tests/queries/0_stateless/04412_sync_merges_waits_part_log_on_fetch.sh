#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that affects all RMT fetches
# no-fasttest: starts background merges/fetches across two replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=rmt_fetch_part_pause_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# Replica 'a' executes the merge locally and assigns the MERGE_PARTS log entry. Replica 'b' uses
# the Manual selector (so it accepts SYSTEM SCHEDULE/SYNC MERGES) and always_fetch_merged_part=1,
# so it satisfies the merge by FETCHING the result part from 'a' instead of merging locally. The
# fetch path commits the fetched part active before it queues its DOWNLOAD_PART part_log row and
# creates no merge list entry, so SYSTEM SYNC MERGES must explicitly wait for that post-commit
# part_log write. insert_keeper_fault_injection_probability=0 keeps part names deterministic.
$CLICKHOUSE_CLIENT -m -q "
    SET insert_keeper_fault_injection_probability = 0;

    DROP TABLE IF EXISTS sm_a SYNC;
    DROP TABLE IF EXISTS sm_b SYNC;

    CREATE TABLE sm_a (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm', 'a')
    ORDER BY x SETTINGS always_fetch_merged_part = 0, old_parts_lifetime = 600;

    CREATE TABLE sm_b (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm', 'b')
    ORDER BY x SETTINGS merge_selector_algorithm = 'Manual', always_fetch_merged_part = 1, old_parts_lifetime = 600;

    INSERT INTO sm_a VALUES (1);
    INSERT INTO sm_a VALUES (2);
    SYSTEM SYNC REPLICA sm_b;
"

# Pause the fetch on 'b' right after the fetched part becomes active but before its DOWNLOAD_PART
# part_log row is written. This holds that window open deterministically (independent of machine
# load) so the test can observe whether SYSTEM SYNC MERGES waits for the part_log write.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Assign the merge on 'a' and schedule it on 'b'; 'b' then fetches all_0_1_1 and blocks at the pause.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SCHEDULE MERGE sm_b PARTS 'all_0_0_0', 'all_1_1_0';
    OPTIMIZE TABLE sm_a FINAL SETTINGS alter_sync = 1, optimize_throw_if_noop = 1;
"

# Prove the fetch actually reached the post-commit/pre-part_log pause window before running
# SYSTEM SYNC MERGES. SYSTEM WAIT FAILPOINT ... PAUSE blocks until a thread is parked at the
# failpoint and returns non-zero (timeout exit 124) if none does. Without this guard a test that
# only polled for the fetched part to become active could pass spuriously: if the fetch had
# already written its DOWNLOAD_PART row before the poll observed the part, the final count would
# be 1 without the fetch wait being the reason. Fail loudly if the fetch never reaches the window.
if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    echo "FAIL: fetch did not reach $FP within 60s"
    exit 1
fi

# Sanity check (after the failpoint is known paused): the fetched part is active on 'b' but its
# DOWNLOAD_PART part_log row has not been written yet.
active=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'sm_b' AND name = 'all_0_1_1' AND active")
[ "$active" = "1" ] || echo "FAIL: expected fetched part all_0_1_1 active on sm_b while paused, got $active"

# Release the pause shortly after, in the background. On a correct server SYSTEM SYNC MERGES below
# blocks until this release (it waits for the part_log write); on a server without the fix it
# returns immediately while the fetch is still paused.
( sleep 3; $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" ) &
releaser=$!

# SYNC MERGES must not return until the DOWNLOAD_PART row is queued, so the flush right after must
# surface it. count = 1 only if SYNC MERGES waited for the post-commit part_log write on the fetch
# path; without the fix SYNC MERGES returns inside the paused window and count = 0.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SYNC MERGES sm_b;
    SYSTEM FLUSH LOGS part_log;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_b'
      AND event_type = 'DownloadPart' AND part_name = 'all_0_1_1';
"

wait "$releaser" 2>/dev/null || true
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE sm_a SYNC;
    DROP TABLE sm_b SYNC;
"
