#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `merge_memory_estimate_per_source_part_column` prices a merge that removes expired rows as a vertical one
# only when `MergeTask::canVerticalTTLDelete` will let it run vertically:
# - a `ReplacingMergeTree` may run such a merge vertically, so it keeps the width of a vertical merge;
# - source parts with lightweight deletes force a horizontal merge, so they get the width of a horizontal one.
#
# The tables have 16 columns, the sorting key `k` and a rows TTL on `d`. The estimate is sized from the
# server's actual memory limit so that a horizontal merge affords exactly two parts, while a vertical merge,
# which merges only `k` and `d` on its horizontal stage, affords all eight parts at once. Every part holds a
# row whose TTL is due, so every merge removes expired values, and a row whose TTL is far in the future, so
# no part is fully expired. `ttl_only_drop_parts = 1` keeps the TTL selector away, so the regular selector,
# which applies the estimate per range, makes every merge.

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

function create()
{
    local table=$1
    local engine=$2

    $CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, d DateTime, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64,
        c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64)
    ENGINE = $engine ORDER BY k TTL d
    SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
        min_parts_to_merge_at_once = 100,
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
        ttl_only_drop_parts = 1,
        vertical_merge_optimize_ttl_delete = 1,
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

    # `min_parts_to_merge_at_once = 100` keeps the regular selector from merging the parts while they are
    # inserted and mutated.
    for i in {0..7}
    do
        $CLICKHOUSE_CLIENT --query "
        INSERT INTO $table SELECT number, if(number % 3 = 0, '2000-01-01 00:00:00', '2100-01-01 00:00:00'),
            number, number, number, number, number, number, number, number, number, number, number, number, number, number
        FROM numbers($((i * 3)), 3);
        "
    done
}

function merge()
{
    local table=$1

    $CLICKHOUSE_CLIENT --query "
    SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
    ALTER TABLE $table MODIFY SETTING min_parts_to_merge_at_once = 2;
    OPTIMIZE TABLE $table;
    "

    wait_for_merges "$table"

    $CLICKHOUSE_CLIENT --query "
    SELECT count() FROM $table WHERE d > '2050-01-01';
    SYSTEM FLUSH LOGS part_log;
    "
}

echo 'replacing'
create t_merge_width_ttl_replacing ReplacingMergeTree
merge t_merge_width_ttl_replacing
# The prediction has to match what the merge did: the merge removing expired rows ran vertically and took
# all eight parts.
$CLICKHOUSE_CLIENT --query "
SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_ttl_replacing' AND event_type = 'MergeParts'
ORDER BY ALL;
DROP TABLE t_merge_width_ttl_replacing;
"

echo 'lightweight delete'
create t_merge_width_ttl_lightweight_delete MergeTree
$CLICKHOUSE_CLIENT --query "
DELETE FROM t_merge_width_ttl_lightweight_delete WHERE k % 3 = 2 SETTINGS lightweight_deletes_sync = 2, lightweight_delete_mode = 'alter_update';
SELECT 'with lightweight deletes', countIf(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_merge_width_ttl_lightweight_delete' AND active;
"
merge t_merge_width_ttl_lightweight_delete
# The parts with lightweight deletes merge horizontally, so they are narrowed to the horizontal width: no
# merge runs horizontally over more than two parts. Once merged, the parts carry neither lightweight deletes
# and may then merge vertically at any width, so those merges are not checked.
$CLICKHOUSE_CLIENT --query "
SELECT countIf(merge_algorithm = 'Horizontal' AND length(merged_from) = 2) > 0, countIf(merge_algorithm = 'Horizontal' AND length(merged_from) > 2)
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_ttl_lightweight_delete' AND event_type = 'MergeParts';
DROP TABLE t_merge_width_ttl_lightweight_delete;
"
