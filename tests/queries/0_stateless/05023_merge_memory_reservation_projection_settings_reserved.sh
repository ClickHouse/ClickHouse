#!/usr/bin/env bash
# Regression test for the RESERVATION side of the merge memory estimate
# (see CompactionStatistics::estimateNeededMemoryForMerge) on a projection that overrides the MergeTree
# writer settings with WITH SETTINGS. A projection is written through
# getSettings(&projection.settings_changes) (see writeProjectionPartImpl), so a projection that raises
# max_compress_block_size allocates bigger write buffers per stream than the parent table's setting
# describes, and the estimate must price the rebuilt projection with the projection's own effective
# settings. Pricing it with the parent's settings under-reserves such a merge, and the admission gate then
# admits more concurrent merges than the reservation bounds.
#
# The observable here is the reservation itself rather than the merge's output: a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint right after it has been selected - and therefore
# after StorageMergeTree::selectPartsToMerge has reserved its estimate - and the reserved amount is read
# from the MergesMutationsMemoryReservation metric while it waits. Two otherwise identical tables are
# measured, one whose projection raises max_compress_block_size and one that inherits the table's, so the
# check does not depend on any absolute size: with the projection's settings ignored, the two merges would
# be priced identically. Each measurement runs in its own clickhouse-local process against its own data
# directory, so the metric only ever reflects that one merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the largest reservation observed while the selected merge is parked on the failpoint.
#
# The data is written by one clickhouse-local process and merged by the next one: a background merge is
# only selected once the parts are older than min_age_to_force_merge_seconds, which leaves the second
# process time to arm the failpoint before the merge is selected. Both processes share nothing but the data
# directory, and the metric is process-wide, so it reflects this one merge alone.
function reserved_for_merge()
{
    local projection_settings="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05023_merge_memory_reservation_projection_settings_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_reserved (k UInt64, v String)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, max_compress_block_size = 1048576,
                 materialize_projections_on_merge = 1,
                 min_age_to_force_merge_seconds = 5, min_age_to_force_merge_on_partition_only = 1;

        INSERT INTO t_merge_mem_reserved SELECT number, repeat('a', 100) FROM numbers(1000);
        INSERT INTO t_merge_mem_reserved SELECT number, repeat('b', 100) FROM numbers(1000, 1000);
        INSERT INTO t_merge_mem_reserved SELECT number, repeat('c', 100) FROM numbers(2000, 1000);

        -- Added after the inserts, so no source part has it and the merge rebuilds it from the merged rows.
        ALTER TABLE t_merge_mem_reserved ADD PROJECTION p_reserved (SELECT k, v ORDER BY v) ${projection_settings};
    " < /dev/null

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;

        -- The background merge is selected - and its estimate reserved - while these sleeps run, and then
        -- parks on the failpoint before it executes, so the reservation is still held when the metric is read.
        SELECT sleepEachRow(3) FROM numbers(3) SETTINGS max_block_size = 1 FORMAT Null;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SELECT sleepEachRow(3) FROM numbers(2) SETTINGS max_block_size = 1 FORMAT Null;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SELECT sleepEachRow(3) FROM numbers(2) SETTINGS max_block_size = 1 FORMAT Null;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';

        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

# A merge that is selected parks on the failpoint until the final SYSTEM DISABLE FAILPOINT, so a zero
# measurement means the background selector did not pick the merge within the observation window at all
# (on a loaded CI machine selection can lag behind min_age_to_force_merge_seconds) - retry the whole
# measurement on fresh data instead of stretching every run's window.
function reserved_for_merge_with_retries()
{
    local result=0
    for _ in 1 2 3
    do
        result=$(reserved_for_merge "$1")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

# max_compress_block_size is clamped by the writer at MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE
# (256 MiB), which is what this override effectively asks for - far above the table's 1 MiB.
with_override=$(reserved_for_merge_with_retries "WITH SETTINGS (max_compress_block_size = 1073741824)")
without_override=$(reserved_for_merge_with_retries "")

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((without_override > 0))"
# The projection's own max_compress_block_size, not the table's, priced its writers.
echo "$((with_override > without_override))"
