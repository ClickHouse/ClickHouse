#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (minio)

# Regression test for the merge memory reservation on an object-storage disk that allows an UNLIMITED
# number of in-flight upload parts (max_inflight_parts_for_one_file = 0). There
# getMultipartUploadMemory reports MultipartUploadMemory::UNLIMITED as the per-stream ceiling, and the
# estimate must keep that "unbounded" meaning while it combines the ceiling with the compressor block:
# adding max_compress_block_size to it without saturating wraps UInt64 around to a tiny number, so the
# per-stream worst case - the upper half of the std::min with the merge's data-volume bound - silently
# becomes the SMALLER of the two and the merge is admitted on a fraction of the memory its writers can
# allocate.
#
# The check is an invariant rather than an absolute size: an unlimited-in-flight disk can never be priced
# BELOW an otherwise identical disk with a bounded in-flight limit, because its worst case is unbounded and
# both merges share the same data-volume bound. With the overflow, the unlimited disk is priced at
# output streams * (max_compress_block_size - 1) instead, far below the bounded one.
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
    local max_inflight_parts="$1"
    local tag="$2"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05054_merge_memory_reservation_inflight_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_inflight (k UInt64, v String)
        ENGINE = MergeTree ORDER BY k
        SETTINGS disk = disk(
                     type = s3,
                     endpoint = 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_${tag}/',
                     access_key_id = 'clickhouse',
                     secret_access_key = 'clickhouse',
                     s3_max_inflight_parts_for_one_file = ${max_inflight_parts}),
                 min_bytes_for_wide_part = 0, max_compress_block_size = 65536,
                 min_age_to_force_merge_seconds = 5, min_age_to_force_merge_on_partition_only = 1;

        INSERT INTO t_merge_mem_inflight SELECT number, repeat('a', 1000) FROM numbers(1000);
        INSERT INTO t_merge_mem_inflight SELECT number, repeat('b', 1000) FROM numbers(1000, 1000);
        INSERT INTO t_merge_mem_inflight SELECT number, repeat('c', 1000) FROM numbers(2000, 1000);
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
    for attempt in 1 2 3
    do
        result=$(reserved_for_merge "$1" "$2_$attempt")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

unlimited_inflight=$(reserved_for_merge_with_retries 0 unlimited)
bounded_inflight=$(reserved_for_merge_with_retries 4 bounded)

# The merge was selected and its estimate reserved before it parked on the failpoint.
echo "$((bounded_inflight > 0))"
# The unbounded per-stream ceiling did not wrap around into an under-reservation.
echo "$((unlimited_inflight >= bounded_inflight))"
