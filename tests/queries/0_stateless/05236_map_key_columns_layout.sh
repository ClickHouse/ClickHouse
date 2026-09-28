#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-fasttest
#
# On-disk layout of a Wide part with a `with_key_columns` Map column: the part contains
# the m.keys stream plus one m.values.<key> and one m.exists.<key> stream per stored key,
# while columns.txt lists only the logical Map column without per-key entries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_kc_layout"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_kc_layout
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0, min_level_for_full_part_storage = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    replace_long_file_name_to_hash = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0
"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_kc_layout"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_kc_layout VALUES (1, {'a': 1, 'b': 2})"

echo "substreams"
${CLICKHOUSE_CLIENT} -q "
SELECT arraySort(arrayFilter(x -> x LIKE 'm.keys%' OR x LIKE 'm.values.%' OR x LIKE 'm.exists.%', substreams))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_kc_layout' AND column = 'm' AND active
"

part_dir=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_kc_layout' AND active
")

has_pattern()
{
    local dir=$1
    local pattern=$2
    if find "$dir" -maxdepth 1 -name "$pattern" | grep -q .
    then
        echo 1
    else
        echo 0
    fi
}

echo "files"
echo "keys=$(has_pattern "$part_dir" 'm.keys.*')"
echo "values_a=$(has_pattern "$part_dir" 'm.values.a*')"
echo "exists_a=$(has_pattern "$part_dir" 'm.exists.a*')"
echo "values_b=$(has_pattern "$part_dir" 'm.values.b*')"
echo "exists_b=$(has_pattern "$part_dir" 'm.exists.b*')"
echo "key_c_values=$(has_pattern "$part_dir" 'm.values.c*')"
echo "key_c_exists=$(has_pattern "$part_dir" 'm.exists.c*')"

echo "columns_txt"
# columns.txt must contain only the logical columns, no per-key streams.
grep -c '^`m`' "$part_dir/columns.txt" | sed 's/^/m_columns=/'
grep -c '^`m\.' "$part_dir/columns.txt" | sed 's/^/per_key_entries=/' || true

echo "percent escaped key"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_kc_layout VALUES (2, {'with space': 3})"
part_dir2=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_kc_layout' AND active ORDER BY name DESC LIMIT 1
")
# The key 'with space' is stored in a percent-escaped file name, so there must be
# value/exists streams and no literal space in file names.
echo "values_escaped=$(has_pattern "$part_dir2" 'm.values.*with*')"
echo "exists_escaped=$(has_pattern "$part_dir2" 'm.exists.*with*')"
echo "literal_space=$(find "$part_dir2" -maxdepth 1 -name '* *' | grep -q . && echo 1 || echo 0)"
${CLICKHOUSE_CLIENT} -q "SELECT m['with space'] FROM t_kc_layout ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_kc_layout"
