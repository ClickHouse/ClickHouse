#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that makes every fetch throw after commit
# no-fasttest: starts background merges/fetches across two replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=rmt_fetch_throw_after_commit_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# Replica 'a' executes the merge locally; replica 'b' uses the Manual selector (so it accepts
# SYSTEM SCHEDULE/SYNC MERGES) and always_fetch_merged_part=1, so it satisfies the merge by
# FETCHING the result part from 'a'. The fetch commits the fetched part active before it queues
# its DOWNLOAD_PART part_log row, and creates no merge list entry. If a post-commit step throws
# (quorum update, cache prewarming, memory limit), the fetch would unwind with the part active but
# no part_log row. SYSTEM SYNC MERGES must still not return until that (failed) DOWNLOAD_PART row
# is queued, otherwise a following FLUSH LOGS part_log can drain the queue before the row is
# pushed. insert_keeper_fault_injection_probability=0 keeps part names deterministic.
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

# rmt_fetch_throw_after_commit_before_part_log forces the fetch to commit all_0_1_1 active and then
# throw in the post-commit/pre-part_log window. With the fix the fetch queues its (failed)
# DOWNLOAD_PART row before unwinding, so the row is present right after SYNC MERGES (count = 1).
# Without the fix the part stays active with no DOWNLOAD_PART row and SYNC MERGES returns early
# (count = 0).
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Assign the merge on 'a' and schedule it on 'b'; 'b' then fetches all_0_1_1 and throws after commit.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SCHEDULE MERGE sm_b PARTS 'all_0_0_0', 'all_1_1_0';
    OPTIMIZE TABLE sm_a FINAL SETTINGS alter_sync = 1, optimize_throw_if_noop = 1;
"

# SYNC MERGES must not return until the (failed) DOWNLOAD_PART row is queued, so the flush right
# after must surface it. enable_parallel_replicas is pinned off only for the part_log count:
# reading system.part_log over parallel replicas changes the aggregation and is unrelated to the
# part_log ordering under test.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SYNC MERGES sm_b;
    SYSTEM FLUSH LOGS part_log;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_b'
      AND event_type = 'DownloadPart' AND part_name = 'all_0_1_1' AND error > 0
    SETTINGS enable_parallel_replicas = 0;
"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE sm_a SYNC;
    DROP TABLE sm_b SYNC;
"
