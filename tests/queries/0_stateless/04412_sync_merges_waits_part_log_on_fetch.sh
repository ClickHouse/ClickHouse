#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that affects all RMT fetches
# no-fasttest: starts background merges/fetches across two replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Replica 'a' executes merges locally and assigns the MERGE_PARTS log entry.
# Replica 'b' uses the Manual selector (so it accepts SYSTEM SCHEDULE/SYNC MERGES) and
# always_fetch_merged_part=1, so it satisfies the merge by FETCHING the result part from 'a'
# instead of merging locally. The fetch path commits the fetched part active before it queues
# its DOWNLOAD_PART part_log row and creates no merge list entry, so SYSTEM SYNC MERGES must
# explicitly wait for that post-commit part_log write.
$CLICKHOUSE_CLIENT -m -q "
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

# Widen the window between 'fetched part becomes active' and 'DOWNLOAD_PART part_log write'
# so the race is deterministic: without the fix SYNC MERGES returns inside this window.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_fetch_part_sleep_before_part_log"

$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SCHEDULE MERGE sm_b PARTS 'all_0_0_0', 'all_1_1_0';
    OPTIMIZE TABLE sm_a FINAL SETTINGS alter_sync = 1, optimize_throw_if_noop = 1;
    SYSTEM SYNC MERGES sm_b;
"

# SYNC MERGES has returned. The DOWNLOAD_PART row for the fetched merged part must already be
# queued, so a flush right after must surface it. Print it to compare against the reference.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS part_log;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_b'
      AND event_type = 'DownloadPart' AND part_name = 'all_0_1_1';
"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_fetch_part_sleep_before_part_log"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE sm_a SYNC;
    DROP TABLE sm_b SYNC;
"
