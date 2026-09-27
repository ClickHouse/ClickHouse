#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database
#
# The same parts merged on the full-column path and the per-key path must agree
# on values, `mapContains`, and `mapKeys` order. Source manifests are first-seen
# (`c` then `b`, then `a`); the merged manifest is Field order. Each key's value
# substream is listed before its exists substream.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_wkc_paths_full"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_wkc_paths_per_key"

${CLICKHOUSE_CLIENT} --multiquery -q "
SET optimize_on_insert = 0;

CREATE TABLE t_wkc_paths_full
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 100,
    replace_long_file_name_to_hash = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

CREATE TABLE t_wkc_paths_per_key
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 3,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    replace_long_file_name_to_hash = 0,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_paths_full;
SYSTEM STOP MERGES t_wkc_paths_per_key;
INSERT INTO t_wkc_paths_full VALUES (1, map('c', 1, 'b', 2));
INSERT INTO t_wkc_paths_full VALUES (2, map('a', 3));
INSERT INTO t_wkc_paths_per_key VALUES (1, map('c', 1, 'b', 2));
INSERT INTO t_wkc_paths_per_key VALUES (2, map('a', 3));
SYSTEM START MERGES t_wkc_paths_full;
SYSTEM START MERGES t_wkc_paths_per_key;
OPTIMIZE TABLE t_wkc_paths_full FINAL;
OPTIMIZE TABLE t_wkc_paths_per_key FINAL;
"

echo "queries"
${CLICKHOUSE_CLIENT} -q "
SELECT
    id,
    mapKeys(m),
    m['a'],
    m['b'],
    m['c'],
    mapContains(m, 'a'),
    mapContains(m, 'b'),
    mapContains(m, 'c')
FROM t_wkc_paths_full
ORDER BY id
"

echo "except"
${CLICKHOUSE_CLIENT} -q "
SELECT count()
FROM
(
    SELECT id, mapKeys(m), m['a'], m['b'], m['c'], mapContains(m, 'a'), mapContains(m, 'b'), mapContains(m, 'c')
    FROM t_wkc_paths_full
    EXCEPT ALL
    SELECT id, mapKeys(m), m['a'], m['b'], m['c'], mapContains(m, 'a'), mapContains(m, 'b'), mapContains(m, 'c')
    FROM t_wkc_paths_per_key
)
"

map_substreams()
{
    local part_dir=$1
    grep -E '^[[:space:]]+m\.(key|exists)_' "$part_dir/columns_substreams.txt" | sed 's/^[[:space:]]*//'
}

part_dir()
{
    local table=$1
    ${CLICKHOUSE_CLIENT} -q "
        SELECT path FROM system.parts
        WHERE database = currentDatabase() AND table = '$table' AND active
    "
}

full_dir=$(part_dir t_wkc_paths_full)
per_key_dir=$(part_dir t_wkc_paths_per_key)

echo "full_substreams"
map_substreams "$full_dir"
echo "per_key_substreams"
map_substreams "$per_key_dir"
echo "full_manifest"
cat "$full_dir/m.key_columns.txt"
echo "per_key_manifest"
cat "$per_key_dir/m.key_columns.txt"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
echo "algorithms"
${CLICKHOUSE_CLIENT} -q "
SELECT merge_algorithm
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_wkc_paths_full' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1
"
${CLICKHOUSE_CLIENT} -q "
SELECT merge_algorithm
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_wkc_paths_per_key' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_wkc_paths_full"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_wkc_paths_per_key"
