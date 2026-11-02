#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `merge_memory_estimate_per_source_part_column` prices a merge as removing expired values only when a TTL of
# the merged part is due. A table with a `GROUP BY` TTL far in the future still merges vertically, so it keeps
# the width of a vertical merge.
#
# The table has 16 columns and the sorting key `k`. The estimate is sized from the server's actual memory limit
# so that a horizontal merge affords exactly two parts, while a vertical merge, which merges only a couple of
# columns on its horizontal stage, affords all eight parts at once.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_group_by_ttl;

CREATE TABLE t_merge_width_group_by_ttl (k UInt64, d DateTime, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64,
    c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64)
ENGINE = MergeTree ORDER BY k TTL d GROUP BY k SET c1 = sum(c1)
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
    vertical_merge_algorithm_min_bytes_to_activate = 0;
"

# `merge_selector_base = 1000` is far above what eight fresh parts reach, so background merges do not take the
# parts, and `OPTIMIZE`, which selects with the base of one, merges them synchronously.
for i in {0..7}
do
    $CLICKHOUSE_CLIENT --query "
    INSERT INTO t_merge_width_group_by_ttl SELECT number, '2100-01-01 00:00:00',
        number, number, number, number, number, number, number, number, number, number, number, number, number, number
    FROM numbers($((i * 3)), 3);
    "
done

$CLICKHOUSE_CLIENT --query "
SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_group_by_ttl' AND active;
OPTIMIZE TABLE t_merge_width_group_by_ttl;
"

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_width_group_by_ttl'")" = 0 ] && break
    sleep 0.1
done

# The merge ran vertically and took all eight parts.
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM t_merge_width_group_by_ttl;
SYSTEM FLUSH LOGS part_log;
SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_group_by_ttl' AND event_type = 'MergeParts'
ORDER BY ALL;
DROP TABLE t_merge_width_group_by_ttl;
"
