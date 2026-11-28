#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of
# the projections of a merge. MergeProjectionsStage merges the parts of one projection at a time, so the
# projection IO is priced at the largest single projection's footprint, not at the sum over all of them.
# Three identical wide projections must therefore reserve exactly as much as one. The same holds for compact
# projections: a finished projection task is released before the next one starts, so the compressors a compact
# writer keeps after finalization do not accumulate.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_merge()
{
    local projections="$1"
    local min_bytes_for_wide_part="${2:-0}"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05394_merge_memory_reservation_projections_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        CREATE TABLE t_merge_mem_projections (k UInt64, a UInt64, b UInt64, s String, ${projections})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = ${min_bytes_for_wide_part}, min_rows_for_wide_part = 0;

        INSERT INTO t_merge_mem_projections SELECT number, number, number, toString(number) FROM numbers(1000);
        INSERT INTO t_merge_mem_projections SELECT number, number, number, toString(number) FROM numbers(1000, 1000);
        INSERT INTO t_merge_mem_projections SELECT number, number, number, toString(number) FROM numbers(2000, 1000);
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_projections MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

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

one_projection=$(reserved_for_merge_with_retries "PROJECTION p1 (SELECT * ORDER BY a)")
three_projections=$(reserved_for_merge_with_retries "PROJECTION p1 (SELECT * ORDER BY a), PROJECTION p2 (SELECT * ORDER BY a), PROJECTION p3 (SELECT * ORDER BY a)")

echo "$((one_projection > 0))"
echo "$((three_projections == one_projection))"

compact_one_projection=$(reserved_for_merge_with_retries "PROJECTION p1 (SELECT * ORDER BY a)" 1000000000)
compact_three_projections=$(reserved_for_merge_with_retries "PROJECTION p1 (SELECT * ORDER BY a), PROJECTION p2 (SELECT * ORDER BY a), PROJECTION p3 (SELECT * ORDER BY a)" 1000000000)

echo "$((compact_one_projection > 0))"
echo "$((compact_three_projections == compact_one_projection))"
