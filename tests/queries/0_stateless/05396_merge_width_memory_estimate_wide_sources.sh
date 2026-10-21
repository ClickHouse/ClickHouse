#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge never produces a part of a more compact type than its source parts (see
# `FutureMergedMutatedPart::assign`), so merging existing `Wide` parts runs vertically even after the
# thresholds of the table changed to choose `Compact` for the result. The estimate has to predict the same and
# price the merge by its key column alone instead of applying the narrow cap of a horizontal merge.
#
# The table has 16 columns and a single key column. The estimate is sized from the server's actual memory
# limit so that a horizontal merge affords two parts and a vertical merge priced by the key affords 32 parts.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_wide_sources;

CREATE TABLE t_merge_width_wide_sources (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
    c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
    merge_selector_algorithm = 'Simple',
    merge_selector_base = 1000,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_level_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

SYSTEM STOP MERGES t_merge_width_wide_sources;
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(0, 1);
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1, 1);
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(2, 1);
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(3, 1);
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(4, 1);
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(5, 1);
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(6, 1);
INSERT INTO t_merge_width_wide_sources SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(7, 1);
SELECT 'before', part_type, count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_wide_sources' AND active GROUP BY part_type;

-- The thresholds now choose \`Compact\` for the result, but the result stays \`Wide\` like its sources.
ALTER TABLE t_merge_width_wide_sources MODIFY SETTING min_bytes_for_wide_part = 1000000000;
SYSTEM START MERGES t_merge_width_wide_sources;

OPTIMIZE TABLE t_merge_width_wide_sources;
"

# `merge_selector_base = 1000` is far above what eight fresh parts reach, so background merges do not take the
# parts, and `OPTIMIZE`, which selects with the base of one, merges them synchronously. A background merge
# appears in `system.merges` only once an executor thread starts it, so the wait below could miss a merge that
# was selected but not started yet. The wait stays for a background merge that still gets through.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_width_wide_sources'")" = 0 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
SELECT sum(k), sum(c15), count() FROM t_merge_width_wide_sources;

SYSTEM FLUSH LOGS part_log;

SELECT merge_algorithm, part_type, max(length(merged_from)) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_wide_sources' AND event_type = 'MergeParts'
GROUP BY merge_algorithm, part_type
ORDER BY ALL;

DROP TABLE t_merge_width_wide_sources;
"
