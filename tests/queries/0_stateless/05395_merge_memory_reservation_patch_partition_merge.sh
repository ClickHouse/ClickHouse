#!/usr/bin/env bash

# Regression test for the merge memory reservation (CompactionStatistics::estimateNeededMemoryForMerge) of a
# merge of patch parts. Such a merge does not run with the table's metadata:
# MergeTreeDataMergerMutator::mergePartsToTemporaryPart replaces it with the synthetic metadata of the source
# patch, which only has the updated columns and the patch system columns. So the estimate must not depend on
# the table's columns the patches do not update: two tables that differ only by columns the UPDATEs never touch
# must reserve exactly the same amount for merging their patch parts.
#
# The reservation is read from the MergesMutationsMemoryReservation metric while a background merge is held
# on the plain_merge_task_pause_before_prepare failpoint (see 05220_merge_memory_reservation_marks_streams).
# The table has a single base part, so the only merge that can be selected is the one of the patch parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function reserved_for_patch_merge()
{
    local extra_columns="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05395_merge_memory_reservation_patch_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SET enable_lightweight_update = 1;

        CREATE TABLE t_merge_mem_patch_partition (id UInt64, c1 UInt64 ${extra_columns})
        ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

        INSERT INTO t_merge_mem_patch_partition (id, c1) SELECT number, number FROM numbers(3000);

        UPDATE t_merge_mem_patch_partition SET c1 = c1 + 1 WHERE id % 3 = 0;
        UPDATE t_merge_mem_patch_partition SET c1 = c1 + 1 WHERE id % 3 = 1;
        UPDATE t_merge_mem_patch_partition SET c1 = c1 + 1 WHERE id % 3 = 2;
    " < /dev/null

    timeout 120 ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        ALTER TABLE t_merge_mem_patch_partition MODIFY SETTING min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
        SYSTEM WAIT FAILPOINT plain_merge_task_pause_before_prepare PAUSE;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

function reserved_for_patch_merge_with_retries()
{
    local result=0
    for _ in 1 2 3
    do
        result=$(reserved_for_patch_merge "$1")
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

extra_columns=""
for i in $(seq 1 50); do extra_columns+=", e$i String DEFAULT 'x'"; done

narrow_table=$(reserved_for_patch_merge_with_retries "")
wide_table=$(reserved_for_patch_merge_with_retries "$extra_columns")

echo "$((narrow_table > 0))"
echo "$((wide_table == narrow_table))"
