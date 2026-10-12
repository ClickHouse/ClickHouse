#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A vertical merge is priced by the columns it merges on the horizontal stage. The columns of a projection
# are merged there only when the merge rebuilds the projection; when every source part already has it, the
# projection parts are merged separately and the parent merge is priced by its key column alone.
#
# The table has 16 columns, a single key column and a projection over 7 other columns. The estimate is sized
# from the server's actual memory limit so that a horizontal merge affords two parts, a vertical merge priced
# by the key alone affords 32 parts, and a vertical merge that also merges the projection columns affords 4.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
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
    local materialize_on_insert=$2

    $CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
        c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64,
        PROJECTION p (SELECT c1, c2, c3, c4, c5, c6, c7 ORDER BY c1))
    ENGINE = MergeTree ORDER BY k
    SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
        min_parts_to_merge_at_once = 2,
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
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
        enable_block_offset_column = 0,
        materialize_projections_on_insert = $materialize_on_insert,
        materialize_projections_on_merge = 1;

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

# Every part has the projection, so the merge merges the projection parts instead of rebuilding the
# projection, and affords all eight parts at once.
echo 'projection in every part'
run t_merge_width_projection_merged 1

# No part has the projection and `materialize_projections_on_merge` rebuilds it, so the merge also merges the
# projection columns on the horizontal stage and affords four parts.
echo 'projection rebuilt'
run t_merge_width_projection_rebuilt 0
