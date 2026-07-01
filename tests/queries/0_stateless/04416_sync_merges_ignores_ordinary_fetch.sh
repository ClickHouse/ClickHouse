#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that pauses every non-detached RMT fetch
# no-fasttest: starts background fetches across two replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=rmt_fetch_part_pause_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# SYSTEM SYNC MERGES waits for an in-flight fetch that satisfies a MERGE_PARTS log entry (the merged
# result part), because that fetch commits the part active before it queues its DOWNLOAD_PART
# part_log row. It must NOT wait for an ordinary GET_PART / ATTACH_PART fetch: those can resolve to a
# covering merged part (via findReplicaHavingCoveringPart) yet are not the scheduled merge and owe no
# success part_log row for it. Waiting on them makes SYNC MERGES hang or time out spuriously.
#
# Replica 'a' merges its two parts into all_0_1_1. Replica 'b' (Manual selector) is then created and
# clones the active part all_0_1_1 from 'a' via an ordinary GET_PART entry, not a MERGE_PARTS entry.
# The scheduled source parts all_0_0_0 / all_1_1_0 are registered on 'b' via SCHEDULE MERGE, and
# all_0_1_1 covers them, so clauses 1 (coverage) and 2 (no in-flight merge list entry) of SYNC MERGES
# are met while the GET_PART fetch is paused. Only the fetch wait (clause 3) differs between builds.
# insert_keeper_fault_injection_probability=0 keeps part names deterministic.
$CLICKHOUSE_CLIENT -m -q "
    SET insert_keeper_fault_injection_probability = 0;

    DROP TABLE IF EXISTS sm_a SYNC;
    DROP TABLE IF EXISTS sm_b SYNC;

    CREATE TABLE sm_a (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm', 'a')
    ORDER BY x SETTINGS always_fetch_merged_part = 0, old_parts_lifetime = 600;

    INSERT INTO sm_a VALUES (1);
    INSERT INTO sm_a VALUES (2);
    OPTIMIZE TABLE sm_a FINAL SETTINGS alter_sync = 1, optimize_throw_if_noop = 1;
"

active_a=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'sm_a' AND name = 'all_0_1_1' AND active")
[ "$active_a" = "1" ] || echo "FAIL: expected merged part all_0_1_1 active on sm_a, got $active_a"

# Pause every non-detached fetch right after the fetched part becomes active but before its
# DOWNLOAD_PART part_log row is written. This holds the GET_PART clone-fetch of all_0_1_1 open
# deterministically.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Create the Manual-selector replica 'b'. On creation it clones the active part all_0_1_1 from 'a'
# via a GET_PART entry (not MERGE_PARTS) and blocks at the pause. Run it in the background because
# the CREATE returns only after the initial clone fetches complete (which are paused here).
$CLICKHOUSE_CLIENT -m -q "
    SET insert_keeper_fault_injection_probability = 0;
    CREATE TABLE sm_b (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm', 'b')
    ORDER BY x SETTINGS merge_selector_algorithm = 'Manual', always_fetch_merged_part = 0, old_parts_lifetime = 600;
" &
creator=$!

# CREATE registers the table in the catalog before it starts cloning, but the statement above does
# not return until the paused clone finishes, so wait for the table to appear before scheduling.
for _ in $(seq 1 600); do
    exists=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'sm_b'")
    [ "$exists" = "1" ] && break
    sleep 0.1
done
[ "$exists" = "1" ] || { echo "FAIL: table sm_b was not created within 60s"; exit 1; }

# Register the scheduled merge for the source parts on 'b' (SCHEDULE MERGE only records the names,
# the parts need not exist locally yet). all_0_1_1 covers them, so coverage is satisfied.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_b PARTS 'all_0_0_0', 'all_1_1_0'"

# Prove the GET_PART fetch actually reached the post-commit/pre-part_log pause window before running
# SYNC MERGES. SYSTEM WAIT FAILPOINT ... PAUSE blocks until a thread is parked at the failpoint and
# returns non-zero (timeout exit 124) if none does. Fail loudly otherwise, so the test cannot pass
# without exercising the paused-fetch window.
if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    echo "FAIL: GET_PART fetch did not reach $FP within 60s"
    exit 1
fi

# Sanity check (after the failpoint is known paused): all_0_1_1 is active on 'b' and it is being
# fetched by an ordinary GET_PART entry, not a MERGE_PARTS entry.
active_b=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'sm_b' AND name = 'all_0_1_1' AND active")
[ "$active_b" = "1" ] || echo "FAIL: expected fetched part all_0_1_1 active on sm_b while paused, got $active_b"
fetch_type=$($CLICKHOUSE_CLIENT -q "SELECT type FROM system.replication_queue WHERE database = currentDatabase() AND table = 'sm_b' AND new_part_name = 'all_0_1_1'")
[ "$fetch_type" = "GET_PART" ] || echo "FAIL: expected all_0_1_1 fetched via GET_PART on sm_b, got '$fetch_type'"

# SYSTEM SYNC MERGES must NOT wait on the paused GET_PART fetch: the scheduled parts are already
# covered, no merge is in the merge list, and the GET_PART fetch owes no DOWNLOAD_PART row for the
# scheduled merge. With the fix SYNC MERGES returns promptly (SYNC_OK). Without it,
# hasInFlightFetchCoveringParts sees all_0_1_1 in currently_fetching_merged_parts and SYNC MERGES
# loops until it times out (SYNC_TIMEOUT).
if $CLICKHOUSE_CLIENT --max_execution_time 5 -q "SYSTEM SYNC MERGES sm_b" 2>/dev/null; then
    echo "SYNC_OK"
else
    echo "SYNC_TIMEOUT"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait "$creator" 2>/dev/null || true

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE sm_a SYNC;
    DROP TABLE sm_b SYNC;
"
