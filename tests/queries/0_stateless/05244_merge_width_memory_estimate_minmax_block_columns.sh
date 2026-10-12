#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A vertical merge that may reduce rows recomputes the min-max index on its horizontal stage, and with
# `part_minmax_index_columns = 'with_block_number_offset'` that index also covers `_block_number` and
# `_block_offset`. `merge_memory_estimate_per_source_part_column` has to price such a merge by those two
# columns as well as by the key: otherwise it allows a wider merge than the real one can keep in memory.
#
# The table is a `ReplacingMergeTree`, which may always reduce rows, with 16 columns and a single key
# column. The estimate is sized from the server's actual memory limit so that a vertical merge priced by
# the key column alone affords 8 parts, and one priced by the key and both block columns affords only two
# (8 / 3, rounded down). A horizontal merge of all 16 columns affords two parts too. Every candidate range
# is eligible for the vertical algorithm, see `05218_merge_width_memory_estimate_vertical`.
#
# Background merges may take some of the parts before `OPTIMIZE` does, so the test checks what every merge
# of the table did rather than how many parts are left: each of them has to be a vertical merge of two parts.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 1 / 8 ))

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

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_minmax_block;

CREATE TABLE t_merge_width_minmax_block (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
    c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64)
ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset',
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
    vertical_merge_algorithm_min_bytes_to_activate = 0;

SYSTEM STOP MERGES t_merge_width_minmax_block;
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1);
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1, 1);
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(2, 1);
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(3, 1);
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(4, 1);
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(5, 1);
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(6, 1);
INSERT INTO t_merge_width_minmax_block SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(7, 1);
SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_minmax_block' AND active;
SYSTEM START MERGES t_merge_width_minmax_block;

OPTIMIZE TABLE t_merge_width_minmax_block;
"

wait_for_merges t_merge_width_minmax_block

$CLICKHOUSE_CLIENT --query "
SELECT sum(k), sum(c15), count() FROM t_merge_width_minmax_block;

SYSTEM FLUSH LOGS part_log;

SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_minmax_block' AND event_type = 'MergeParts';

DROP TABLE t_merge_width_minmax_block;
"
