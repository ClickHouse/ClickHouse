#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge is priced as a vertical one, by its key columns alone, only when it does run vertically.
# `MergeTask` counts only the physical columns towards `vertical_merge_algorithm_min_columns_to_activate`,
# so alias columns must not make the prediction vertical.
#
# The table has two physical columns, one of them the key, and many alias columns. With
# `vertical_merge_algorithm_min_columns_to_activate = 2` it merges horizontally, so the merge is priced by
# both physical columns. The estimate is sized from the server's actual memory limit so that this affords
# exactly two parts; pricing the merge as a vertical one would afford four. The block number and offset
# columns are disabled, because `MergeTask` gathers them too, and would make the merge vertical.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 2 / 2 ))

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_alias;

CREATE TABLE t_merge_width_alias (k UInt64, c UInt64,
    a1 UInt64 ALIAS c + 1, a2 UInt64 ALIAS c + 2, a3 UInt64 ALIAS c + 3, a4 UInt64 ALIAS c + 4,
    a5 UInt64 ALIAS c + 5, a6 UInt64 ALIAS c + 6, a7 UInt64 ALIAS c + 7, a8 UInt64 ALIAS c + 8)
ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_columns_to_activate = 2,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

SYSTEM STOP MERGES t_merge_width_alias;
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(0, 1);
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(1, 1);
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(2, 1);
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(3, 1);
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(4, 1);
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(5, 1);
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(6, 1);
INSERT INTO t_merge_width_alias SELECT number, number FROM numbers(7, 1);
SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_alias' AND active;
SYSTEM START MERGES t_merge_width_alias;

OPTIMIZE TABLE t_merge_width_alias;
"

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_width_alias'")" = 0 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
SELECT sum(k), sum(a8), count() FROM t_merge_width_alias;

SYSTEM FLUSH LOGS part_log;

SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_alias' AND event_type = 'MergeParts'
ORDER BY ALL;

DROP TABLE t_merge_width_alias;
"
