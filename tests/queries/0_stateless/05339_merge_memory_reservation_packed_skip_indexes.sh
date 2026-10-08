#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of
# skip indexes packed into `skp_idx.packed`. With packed_skip_index_max_bytes > 0 the data file and the marks
# file of every skip-index substream keep everything written into them in an in-memory accumulator until the
# substream spills past the threshold or is committed to the archive at finalize, so a merge holds up to
# about 2 * packed_skip_index_max_bytes per substream in addition to its buffers.
#
# - When the merge materializes an index the source parts do not store (ADD INDEX without MATERIALIZE INDEX),
#   its volume is unknown, and the reservation must grow by at least 2 * threshold per substream.
# - When every source part already stores the index, the accumulators are bounded by the (tiny) index bytes
#   of the source parts, and the reservation must not grow by anything close to the threshold.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while the selected background
# merge is held on the plain_merge_task_pause_before_prepare failpoint, each measurement in its own
# clickhouse-local process against its own data directory, as in 05220_merge_memory_reservation_marks_streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

THRESHOLD=$((4 * 1024 * 1024))
INDEXES=4

# $1: packed_skip_index_max_bytes, $2: 1 to add the indexes after the inserts (the merge materializes them),
# 0 to create them with the table (every source part stores them).
function reserved_for_merge()
{
    local packed_skip_index_max_bytes="$1"
    local materialize_on_merge="$2"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05339_merge_memory_reservation_packed_XXXXXX")

    local indexes="INDEX ia a TYPE minmax, INDEX ib b TYPE minmax, INDEX ic c TYPE minmax, INDEX id d TYPE minmax"
    local create_indexes=", ${indexes}"
    local add_indexes=""
    if [ "$materialize_on_merge" = 1 ]; then
        create_indexes=""
        add_indexes="ALTER TABLE t_merge_mem_packed ADD ${indexes//, INDEX/, ADD INDEX};"
    fi

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_packed (k UInt64, a UInt64, b UInt64, c UInt64, d UInt64 ${create_indexes})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, materialize_skip_indexes_on_merge = 1,
                 packed_skip_index_max_bytes = ${packed_skip_index_max_bytes};

        INSERT INTO t_merge_mem_packed SELECT number, number, number, number, number FROM numbers(1000);
        INSERT INTO t_merge_mem_packed SELECT number, number, number, number, number FROM numbers(1000, 1000);
        INSERT INTO t_merge_mem_packed SELECT number, number, number, number, number FROM numbers(2000, 1000);

        ${add_indexes}
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_packed MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

# A zero (empty) measurement means the background selector did not pick the merge before the timeout.
function reserved_for_merge_with_retries()
{
    local result=0
    for _ in 1 2 3
    do
        result=$(reserved_for_merge "$1" "$2")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

materialized_unpacked=$(reserved_for_merge_with_retries 0 1)
materialized_packed=$(reserved_for_merge_with_retries "$THRESHOLD" 1)
stored_unpacked=$(reserved_for_merge_with_retries 0 0)
stored_packed=$(reserved_for_merge_with_retries "$THRESHOLD" 0)

echo "$((materialized_unpacked > 0 && stored_unpacked > 0))"
# Both accumulators (data and marks) of every substream of an index materialized by the merge are reserved.
echo "$((materialized_packed - materialized_unpacked >= 2 * 2 * THRESHOLD * INDEXES))"
# The accumulators of indexes stored in every source part are bounded by their index bytes.
echo "$((stored_packed >= stored_unpacked && stored_packed - stored_unpacked < THRESHOLD))"
