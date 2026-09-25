#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-fasttest
#
# Compact with_key_columns keeps key data inside data.bin. keys_info is a
# Compact prefix stream, not a sidecar file. There are no per-key .bin files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_wkc_compact_files"

${CLICKHOUSE_CLIENT} -q "
SET optimize_on_insert = 0;
CREATE TABLE t_wkc_compact_files
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
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2,
    replace_long_file_name_to_hash = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0
"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_wkc_compact_files"
${CLICKHOUSE_CLIENT} -q "
SET optimize_on_insert = 0;
INSERT INTO t_wkc_compact_files VALUES (1, map('a', 1)), (2, map('a', 2, 'b', 10)), (3, map('c', 3)), (4, map())
"

echo "part type"
${CLICKHOUSE_CLIENT} -q "
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_compact_files' AND active
"

echo "substreams"
${CLICKHOUSE_CLIENT} -q "
SELECT arraySort(arrayFilter(x -> startsWith(x, 'm.'), substreams))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wkc_compact_files' AND column = 'm' AND active
"

part_dir=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_wkc_compact_files' AND active
")

has_pattern()
{
    local pattern=$1
    if find "$part_dir" -maxdepth 1 -name "$pattern" | grep -q .
    then
        echo 1
    else
        echo 0
    fi
}

echo "files"
echo "data_bin=$(has_pattern 'data.bin')"
echo "compact_marks=$(has_pattern 'data.*mrk4')"
echo "keys_info_sidecar=$(has_pattern 'm.keys_info*')"
echo "key_a_bin=$(has_pattern 'm.key_a.bin')"
echo "key_b_bin=$(has_pattern 'm.key_b.bin')"
echo "key_presence_bin=$(has_pattern 'm.key_presence.bin')"
echo "template=$(has_pattern '*template*')"

echo "columns_substreams for m"
grep -E '^\s+m\.' "$part_dir/columns_substreams.txt"

echo "missing key is default"
${CLICKHOUSE_CLIENT} -q "SELECT m['missing'], m.exists_missing FROM t_wkc_compact_files ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_wkc_compact_files"
