#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) on
# the marks side of the writer streams. Every MergeTreeWriterStream eagerly creates TWO buffer chains: the
# data chain (plain_file / compressor, sized by max_compress_block_size) and the marks chain
# (marks_file / marks_compressor, sized by marks_compress_block_size), and on object storage the marks file
# has its own multipart upload state as well. An estimate that models only the data half is short by the
# whole marks side for every wide stream and is not an upper bound on the buffers the merge allocates.
#
# The check is an invariant: the same table and data must reserve strictly more with a larger
# marks_compress_block_size, because the marks compressor block of every stream grows with it. Without the
# marks side the setting has no effect on the estimate at all.
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
# only selected once min_age_to_force_merge_seconds is set, and the second process sets it only after it has
# armed the failpoint, so the merge cannot be selected (and run to completion unobserved) before that.
function reserved_for_merge()
{
    local marks_compress_block_size="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05220_merge_memory_reservation_marks_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_marks (k UInt64, a UInt64, b UInt64, c UInt64, d String)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, compress_marks = 1,
                 marks_compress_block_size = ${marks_compress_block_size};

        INSERT INTO t_merge_mem_marks SELECT number, number, number, number, toString(number) FROM numbers(1000);
        INSERT INTO t_merge_mem_marks SELECT number, number, number, number, toString(number) FROM numbers(1000, 1000);
        INSERT INTO t_merge_mem_marks SELECT number, number, number, number, toString(number) FROM numbers(2000, 1000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;

        -- The background merge is selected - and its estimate reserved - once its parts are older than
        -- min_age_to_force_merge_seconds, and then parks on the failpoint before it executes, so the
        -- reservation is still held when the metric is read. The setting is enabled only now, after the
        -- failpoint is armed: with a fixed age set at creation, a slow run (e.g. under a sanitizer) could
        -- select and complete the merge before the failpoint is armed, and the wait below would hang.
        ALTER TABLE t_merge_mem_marks MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';

        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

# The measurement waits until the selected merge parks on the failpoint, so a zero (empty) measurement means
# the background selector did not pick the merge before the timeout at all - retry the whole measurement on
# fresh data.
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

small_marks_block=$(reserved_for_merge_with_retries 65536)
large_marks_block=$(reserved_for_merge_with_retries 1048576)

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((small_marks_block > 0))"
# The marks compressor block of every stream is part of the reservation.
echo "$((large_marks_block > small_marks_block))"
