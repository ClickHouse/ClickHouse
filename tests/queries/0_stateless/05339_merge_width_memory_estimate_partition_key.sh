#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A vertical merge is priced by the columns it merges on the horizontal stage. The columns of the partition
# key are merged there only when the merge may reduce rows and has to recompute the min-max index; an ordinary
# merge takes the min-max index from the source parts and is priced by its sorting key alone.
#
# The table has 16 columns, a single sorting key column and a partition key over 7 other columns that puts
# every row into the same partition. The estimate is sized from the server's actual memory limit so that a
# horizontal merge affords two parts, a vertical merge priced by the sorting key alone affords 32 parts, and a
# vertical merge that also merges the partition key columns affords 4.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

# `merge_selector_base = 1000` is far above what eight fresh parts reach, so background merges do not take the
# parts, and `OPTIMIZE`, which selects with the base of one, merges them synchronously. A background merge
# appears in `system.merges` only once an executor thread starts it, so the wait below could miss a merge that
# was selected but not started yet. The wait stays for a background merge that still gets through.
function wait_for_merges()
{
    local table=$1
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '$table'")" = 0 ] && return
        sleep 0.1
    done
    echo "Merges of $table did not finish"
}

function run()
{
    local table=$1
    local engine=$2

    $CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
        c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64)
    ENGINE = $engine ORDER BY k
    PARTITION BY (c1 > 100, c2 > 100, c3 > 100, c4 > 100, c5 > 100, c6 > 100, c7 > 100)
    SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
        min_parts_to_merge_at_once = 2,
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
        merge_selector_algorithm = 'Simple',
        merge_selector_base = 1000,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        min_rows_for_full_part_storage = 0,
        allow_vertical_merges_from_compact_to_wide_parts = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        enable_vertical_merge_algorithm = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_bytes_to_activate = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(0, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(2, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(3, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(4, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(5, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(6, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(7, 1);
    SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
    SYSTEM START MERGES $table;

    OPTIMIZE TABLE $table;
    "

    wait_for_merges "$table"

    $CLICKHOUSE_CLIENT --query "
    SELECT sum(k), sum(c15), count() FROM $table;

    SYSTEM FLUSH LOGS part_log;

    SELECT merge_algorithm, max(length(merged_from)) <= 4, max(length(merged_from)) = 8 FROM system.part_log
    WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'
    GROUP BY merge_algorithm
    ORDER BY ALL;

    DROP TABLE $table;
    "
}

# An ordinary merge does not recompute the min-max index and affords all eight parts at once.
echo 'ordinary'
run t_merge_width_partition_key_ordinary MergeTree

# A merge of `ReplacingMergeTree` may reduce rows, so it recomputes the min-max index on the horizontal stage
# and affords four parts.
echo 'replacing'
run t_merge_width_partition_key_replacing ReplacingMergeTree
