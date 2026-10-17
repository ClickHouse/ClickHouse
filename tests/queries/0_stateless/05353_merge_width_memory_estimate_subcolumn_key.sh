#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A vertical merge is priced by the storage columns its horizontal stage merges, like `MergeTask` does:
# a sorting key over several subcolumns of one column keeps that one column on the horizontal stage.
#
# The table has four physical columns and is sorted by two subcolumns of the tuple `t`, so a vertical merge
# merges `t` alone on the horizontal stage. The estimate is sized from the server's actual memory limit so
# that a horizontal merge affords two parts and a vertical one with one key column affords four; counting
# `t.a` and `t.b` as two key columns would afford two. The block number and offset columns are disabled,
# because they would be merged on the horizontal stage too.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 4 ))

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_subcolumn_key;

CREATE TABLE t_merge_width_subcolumn_key (t Tuple(a UInt64, b UInt64), c1 UInt64, c2 UInt64, c3 UInt64)
ENGINE = MergeTree ORDER BY (t.a, t.b)
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
    enable_block_offset_column = 0;

SYSTEM STOP MERGES t_merge_width_subcolumn_key;
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(0, 1);
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(1, 1);
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(2, 1);
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(3, 1);
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(4, 1);
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(5, 1);
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(6, 1);
INSERT INTO t_merge_width_subcolumn_key SELECT (number, number), number, number, number FROM numbers(7, 1);
SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_subcolumn_key' AND active;
SYSTEM START MERGES t_merge_width_subcolumn_key;

OPTIMIZE TABLE t_merge_width_subcolumn_key;
"

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_width_subcolumn_key'")" = 0 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
SELECT sum(t.a), sum(c3), count() FROM t_merge_width_subcolumn_key;

SYSTEM FLUSH LOGS part_log;

SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_subcolumn_key' AND event_type = 'MergeParts'
ORDER BY ALL;

DROP TABLE t_merge_width_subcolumn_key;
"
