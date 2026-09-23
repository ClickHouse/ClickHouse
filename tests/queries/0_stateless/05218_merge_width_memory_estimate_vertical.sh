#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `merge_memory_estimate_per_source_part_column` narrows a merge that would hold every column of every
# source part at once. A vertical merge holds only the key columns at once and gathers the rest one at a
# time, so it is priced by its key columns alone and keeps its width.
#
# The table has 16 columns and a single key column. The estimate is sized from the server's actual memory
# limit so that a horizontal merge affords exactly two parts, while a vertical merge of the same table
# affords 16 times as many. Every candidate range is eligible for the vertical algorithm as soon as it is
# allowed at all: the parts are wide in full storage, the row, byte and column thresholds are lowered to
# nothing, and compact source parts would not rule the algorithm out. Every one of these settings is
# randomized by the test runner, and any of them could turn the vertical range into a horizontal one.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_vertical;

CREATE TABLE t_merge_width_vertical (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
    c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64)
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
    enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

SYSTEM STOP MERGES t_merge_width_vertical;
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1);
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1, 1);
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(2, 1);
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(3, 1);
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(4, 1);
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(5, 1);
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(6, 1);
INSERT INTO t_merge_width_vertical SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(7, 1);
SYSTEM START MERGES t_merge_width_vertical;

SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_vertical' AND active;

SET optimize_throw_if_noop = 1;

-- Horizontal merges only: the estimate affords two parts, so one merge takes two of the eight.
OPTIMIZE TABLE t_merge_width_vertical;
SELECT 'horizontal capped', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_vertical' AND active;

-- Now every range will merge vertically, and a vertical merge of this table affords all the parts.
ALTER TABLE t_merge_width_vertical MODIFY SETTING enable_vertical_merge_algorithm = 1;
OPTIMIZE TABLE t_merge_width_vertical;
SELECT 'vertical uncapped', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_vertical' AND active;

SELECT sum(k), sum(c15), count() FROM t_merge_width_vertical;

SYSTEM FLUSH LOGS part_log;

-- The prediction has to match what the merge did: the wide merge did run vertically.
SELECT merge_algorithm, length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_vertical' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds;

DROP TABLE t_merge_width_vertical;
"
