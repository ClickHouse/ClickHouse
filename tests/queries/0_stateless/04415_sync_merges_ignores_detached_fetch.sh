#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that pauses every detached fetch
# no-fasttest: starts background merges/fetches across two replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=rmt_fetch_detached_part_pause

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# Replica 'a' (default selector) merges locally and assigns the MERGE_PARTS log entry. Replica 'b'
# uses the Manual selector so it accepts SYSTEM SCHEDULE/SYNC MERGES. SYSTEM SYNC MERGES waits for an
# in-flight fetch whose result part covers a scheduled source part, because a merge-satisfying fetch
# commits the part active before it queues its DOWNLOAD_PART part_log row. But a detached fetch
# (SYSTEM FETCH PART/PARTITION) and a shared-storage move queue no success part_log row, so an
# unrelated detached fetch of a covering part must NOT make SYNC MERGES wait or time out.
# insert_keeper_fault_injection_probability=0 keeps part names deterministic.
$CLICKHOUSE_CLIENT -m -q "
    SET insert_keeper_fault_injection_probability = 0;

    DROP TABLE IF EXISTS sm_a SYNC;
    DROP TABLE IF EXISTS sm_b SYNC;

    CREATE TABLE sm_a (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm', 'a')
    ORDER BY x SETTINGS old_parts_lifetime = 600;

    CREATE TABLE sm_b (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm', 'b')
    ORDER BY x SETTINGS merge_selector_algorithm = 'Manual', old_parts_lifetime = 600;

    INSERT INTO sm_a VALUES (1);
    INSERT INTO sm_a VALUES (2);
    SYSTEM SYNC REPLICA sm_b;
"

# Schedule the merge on 'b' (registers the scheduled source parts) and produce the merged part on
# 'a'. 'b' then replicates all_0_1_1 and SYNC REPLICA drains its queue, so the scheduled merge's
# part_log row is written and no merge is left in flight: clauses 1 and 2 of SYNC MERGES are met.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SCHEDULE MERGE sm_b PARTS 'all_0_0_0', 'all_1_1_0';
    OPTIMIZE TABLE sm_a FINAL SETTINGS alter_sync = 1, optimize_throw_if_noop = 1;
    SYSTEM SYNC REPLICA sm_b;
"

active=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'sm_b' AND name = 'all_0_1_1' AND active")
[ "$active" = "1" ] || echo "FAIL: expected merged part all_0_1_1 active on sm_b, got $active"

# Start an UNRELATED detached fetch of a covering part and pause it while it holds all_0_1_1 in
# currently_fetching_parts. SYSTEM FETCH PART goes to the detached directory and queues no
# DOWNLOAD_PART success row, so SYNC MERGES has nothing to wait for here.
zk_path=$($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'sm_a'")
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT -q "ALTER TABLE sm_b FETCH PART 'all_0_1_1' FROM '$zk_path'" >/dev/null 2>&1 &
fetcher=$!

# Prove the detached fetch is actually parked (all_0_1_1 is in currently_fetching_parts) before
# running SYNC MERGES. SYSTEM WAIT FAILPOINT ... PAUSE blocks until a thread is parked at the
# failpoint and returns non-zero (timeout exit 124) if none does. Fail loudly otherwise, so the
# test cannot pass without exercising the detached-fetch window.
if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    echo "FAIL: detached fetch did not reach $FP within 60s"
    exit 1
fi

# SYSTEM SYNC MERGES must NOT wait on the detached fetch: the scheduled merge's part_log row is
# already written and the detached fetch owes no DOWNLOAD_PART row. With the fix SYNC MERGES returns
# promptly (SYNC_OK). Without it, hasInFlightFetchCoveringParts sees the detached fetch in
# currently_fetching_parts and SYNC MERGES loops until it times out (SYNC_TIMEOUT).
if $CLICKHOUSE_CLIENT --max_execution_time 5 -q "SYSTEM SYNC MERGES sm_b" 2>/dev/null; then
    echo "SYNC_OK"
else
    echo "SYNC_TIMEOUT"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait "$fetcher" 2>/dev/null || true

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE sm_a SYNC;
    DROP TABLE sm_b SYNC;
"
