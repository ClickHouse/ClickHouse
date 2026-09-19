#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) on
# the count-based adaptive write buffer rule. MergeTreeDataPartWriterWide::addStreams opens every stream
# with an adaptive write buffer (starting at adaptive_write_buffer_initial_size instead of the full
# max_compress_block_size) once the number of DISTINCT streams the writer opens in the part reaches
# min_columns_to_activate_adaptive_write_buffer - it compares streams_to_open_in_part, not the number of
# columns. A single wide column with many substreams (here a Tuple of 20 elements) therefore writes through
# adaptive buffers, and an estimate keyed off the column count would price it at full-size eager buffers per
# stream the writer never allocates: the over-reservation / starvation class the reservation must avoid.
#
# The check is an invariant: the same two-column table (a key and the 20-element Tuple, 21 streams) must
# reserve strictly less with min_columns_to_activate_adaptive_write_buffer = 16 (every stream adaptive)
# than with the rule disabled (0, every stream at the full size). With a column-count rule the two are
# priced identically, since two columns never reach the threshold.
#
# The reservation itself is the observable: a background merge is held on the
# plain_merge_task_pause_before_prepare failpoint right after StorageMergeTree::selectPartsToMerge has
# reserved its estimate, and the reserved amount is read from the MergesMutationsMemoryReservation metric
# while it waits. Each measurement runs in its own clickhouse-local process against its own data directory,
# so the process-wide metric only ever reflects that one merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the largest reservation observed while the selected merge is parked on the failpoint.
#
# The data is written by one clickhouse-local process and merged by the next one: a background merge is
# only selected once the parts are older than min_age_to_force_merge_seconds, which leaves the second
# process time to arm the failpoint before the merge is selected.
function reserved_for_merge()
{
    local min_columns_to_activate="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05219_merge_memory_reservation_adaptive_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_adaptive
        (
            k UInt64,
            t Tuple(e1 UInt64, e2 UInt64, e3 UInt64, e4 UInt64, e5 UInt64, e6 UInt64, e7 UInt64, e8 UInt64, e9 UInt64, e10 UInt64,
                    e11 UInt64, e12 UInt64, e13 UInt64, e14 UInt64, e15 UInt64, e16 UInt64, e17 UInt64, e18 UInt64, e19 UInt64, e20 UInt64)
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0,
                 min_columns_to_activate_adaptive_write_buffer = ${min_columns_to_activate},
                 adaptive_write_buffer_initial_size = 16384, max_compress_block_size = 1048576,
                 min_age_to_force_merge_seconds = 5, min_age_to_force_merge_on_partition_only = 1;

        INSERT INTO t_merge_mem_adaptive SELECT number, tuple(number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number) FROM numbers(1000);
        INSERT INTO t_merge_mem_adaptive SELECT number, tuple(number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number) FROM numbers(1000, 1000);
        INSERT INTO t_merge_mem_adaptive SELECT number, tuple(number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number) FROM numbers(2000, 1000);
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

adaptive=$(reserved_for_merge_with_retries 16)
full_size=$(reserved_for_merge_with_retries 0)

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((adaptive > 0))"
# 21 streams of two columns reach the threshold of 16: the writer opens them adaptively and so must the estimate.
echo "$((adaptive < full_size))"
