#!/usr/bin/env bash
# Regression test for the RESERVATION side of the merge memory estimate
# (see CompactionStatistics::estimateNeededMemoryForMerge) on a rebuilt projection whose READ-BACK merge
# writes a part in a different format than the temporary parts it reads. The rebuild squashes the projected
# rows into chunks and formats every temporary part from its own chunk's size, but MergeProjectionPartsTask
# then batches the temporary parts into nested merges whose FutureMergedMutatedPart::assign re-runs
# choosePartFormat on the summed bytes and rows, so the final projection part comes out Wide once the whole
# rebuilt volume clears the wide-part thresholds - allocating per-substream write buffers the Compact
# temporary parts say nothing about. The estimate must price the read-back writer by that final format;
# pricing it by the temporary parts' format under-reserves the merge.
#
# The observable is the reservation itself: a background merge is held on the
# plain_merge_task_pause_before_prepare failpoint right after StorageMergeTree::selectPartsToMerge has
# reserved its estimate, and the reserved amount is read from the MergesMutationsMemoryReservation metric
# while it waits. The two measurements differ only in the projection's OWN wide-part threshold (a projection
# overrides MergeTree writer settings with WITH SETTINGS), so the base output part, the data and the
# temporary Compact projection parts are identical in both, and only the read-back part's format changes.
# With the read-back priced from the temporary parts, both merges would be priced identically.
# Each measurement runs in its own clickhouse-local process against its own data directory, so the metric
# only ever reflects that one merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the largest reservation observed while the selected merge is parked on the failpoint.
#
# The data is written by one clickhouse-local process and merged by the next one: a background merge is
# only selected once the parts are older than min_age_to_force_merge_seconds, which leaves the second
# process time to arm the failpoint before the merge is selected. Both processes share nothing but the data
# directory, and the metric is process-wide, so it reflects this one merge alone. The squash thresholds are
# query-level settings the merge picks up from its background context; setting them this small in the
# merging process makes the rebuild flush ~1 MB Compact chunks, so the read-back really merges several
# temporary parts into one final projection part.
function reserved_for_merge()
{
    local projection_min_bytes_for_wide_part="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05024_merge_memory_reservation_read_back_format_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_read_back (k UInt64, v String)
        ENGINE = MergeTree ORDER BY k
        SETTINGS materialize_projections_on_merge = 1,
                 min_age_to_force_merge_seconds = 5, min_age_to_force_merge_on_partition_only = 1;

        INSERT INTO t_merge_mem_read_back SELECT number, repeat('a', 600) FROM numbers(8000);
        INSERT INTO t_merge_mem_read_back SELECT number, repeat('b', 600) FROM numbers(8000, 8000);
        INSERT INTO t_merge_mem_read_back SELECT number, repeat('c', 600) FROM numbers(16000, 8000);

        -- The threshold belongs to the projection alone, so the base output part is identical in both
        -- measurements: every temporary projection part stays Compact (its chunk is ~1 MB), while the whole
        -- rebuilt volume - ~14 MB - is above the threshold only in the first measurement.
        ALTER TABLE t_merge_mem_read_back ADD PROJECTION p_read_back (SELECT k, v ORDER BY v)
            WITH SETTINGS (min_bytes_for_wide_part = ${projection_min_bytes_for_wide_part});
    " < /dev/null

    ${CLICKHOUSE_LOCAL} --path "$data_dir" --min_insert_block_size_rows=2000 --min_insert_block_size_bytes=1000000 -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;

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

# 10 MiB: below the whole rebuilt volume, above every temporary chunk - the read-back part is Wide.
wide_read_back=$(reserved_for_merge_with_retries 10485760)
# 1 GB: the read-back part stays Compact, like the temporary parts.
compact_read_back=$(reserved_for_merge_with_retries 1000000000)

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((compact_read_back > 0))"
# The read-back writer was priced by the format of the part it writes, not of the parts it reads.
echo "$((wide_read_back > compact_read_back))"
