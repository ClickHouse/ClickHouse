#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) on
# a Nested layout. With share_nested_offsets the columns of one Nested object share a single offsets stream
# (n.size0): every sibling column records that stream in columns_substreams.txt under its own name, but the
# wide-part writer opens it once for the whole part (MergeTreeDataPartWriterWide::addStreams skips a stream
# name it already created). Summing the per-column substream counts therefore prices the shared offsets once
# per sibling - a full write buffer pair each - and over-reserves every Nested table.
#
# The check is an invariant: a Nested(a, b, c, d) table opens strictly fewer streams (4 data + 1 shared
# offsets) than four independent Array columns holding the same data (4 data + 4 offsets), so its merge
# must reserve strictly less. Without the deduplication both are priced at the same stream count.
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
    local columns="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05218_merge_memory_reservation_nested_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_nested (k UInt64, ${columns})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, share_nested_offsets = 1,
                 min_age_to_force_merge_seconds = 5, min_age_to_force_merge_on_partition_only = 1;

        INSERT INTO t_merge_mem_nested SELECT number, arr, arr, arr, arr FROM (SELECT number, range(number % 7) AS arr FROM numbers(1000));
        INSERT INTO t_merge_mem_nested SELECT number, arr, arr, arr, arr FROM (SELECT number, range(number % 7) AS arr FROM numbers(1000, 1000));
        INSERT INTO t_merge_mem_nested SELECT number, arr, arr, arr, arr FROM (SELECT number, range(number % 7) AS arr FROM numbers(2000, 1000));
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

nested=$(reserved_for_merge_with_retries "n Nested(a UInt64, b UInt64, c UInt64, d UInt64)")
arrays=$(reserved_for_merge_with_retries "a Array(UInt64), b Array(UInt64), c Array(UInt64), d Array(UInt64)")

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((arrays > 0))"
# The shared offsets stream of the Nested columns is priced once, not once per sibling column.
echo "$((nested < arrays))"
