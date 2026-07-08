#!/usr/bin/env bash
# Tags: no-darwin

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="$CLICKHOUSE_CLIENT --enable_variant_type=1"

# Verify that after RENAME COLUMN with a flipped escape_variant_subcolumn_filenames setting,
# the .bin filenames on disk stay consistent with columns_substreams.txt and CHECK TABLE passes.

echo "Case 1: escaping 0 -> 1 with RENAME COLUMN"
$CH -q "DROP TABLE IF EXISTS test_rename_escape"
$CH -q "CREATE TABLE test_rename_escape (v Variant(Tuple(a UInt32, b UInt32))) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=0, replace_long_file_name_to_hash=0"
$CH -q "INSERT INTO test_rename_escape SELECT tuple(1, 2)::Tuple(a UInt32, b UInt32)"

part_path=$($CH -q "SELECT path FROM system.parts WHERE table = 'test_rename_escape' AND database = currentDatabase() AND active")

echo "Before rename .bin files:"
ls "$part_path" | grep '\.bin$' | sort
echo "Before rename columns_substreams.txt:"
cat "${part_path}columns_substreams.txt"

$CH -q "ALTER TABLE test_rename_escape MODIFY SETTING escape_variant_subcolumn_filenames=1"
$CH -q "ALTER TABLE test_rename_escape RENAME COLUMN v TO w SETTINGS mutations_sync=2"

part_path=$($CH -q "SELECT path FROM system.parts WHERE table = 'test_rename_escape' AND database = currentDatabase() AND active")

echo "After rename .bin files:"
ls "$part_path" | grep '\.bin$' | sort
echo "After rename columns_substreams.txt:"
cat "${part_path}columns_substreams.txt"

echo "CHECK TABLE:"
$CH -q "CHECK TABLE test_rename_escape" | cut -f2

echo "Data:"
$CH -q "SELECT w, w.\`Tuple(a UInt32, b UInt32)\`.a, w.\`Tuple(a UInt32, b UInt32)\`.b FROM test_rename_escape"

$CH -q "DROP TABLE test_rename_escape"

echo "Case 2: escaping 1 -> 0 with RENAME COLUMN"
$CH -q "DROP TABLE IF EXISTS test_rename_escape"
$CH -q "CREATE TABLE test_rename_escape (v Variant(Tuple(a UInt32, b UInt32))) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0, escape_variant_subcolumn_filenames=1, replace_long_file_name_to_hash=0"
$CH -q "INSERT INTO test_rename_escape SELECT tuple(3, 4)::Tuple(a UInt32, b UInt32)"

part_path=$($CH -q "SELECT path FROM system.parts WHERE table = 'test_rename_escape' AND database = currentDatabase() AND active")

echo "Before rename .bin files:"
ls "$part_path" | grep '\.bin$' | sort
echo "Before rename columns_substreams.txt:"
cat "${part_path}columns_substreams.txt"

$CH -q "ALTER TABLE test_rename_escape MODIFY SETTING escape_variant_subcolumn_filenames=0"
$CH -q "ALTER TABLE test_rename_escape RENAME COLUMN v TO w SETTINGS mutations_sync=2"

part_path=$($CH -q "SELECT path FROM system.parts WHERE table = 'test_rename_escape' AND database = currentDatabase() AND active")

echo "After rename .bin files:"
ls "$part_path" | grep '\.bin$' | sort
echo "After rename columns_substreams.txt:"
cat "${part_path}columns_substreams.txt"

echo "CHECK TABLE:"
$CH -q "CHECK TABLE test_rename_escape" | cut -f2

echo "Data:"
$CH -q "SELECT w, w.\`Tuple(a UInt32, b UInt32)\`.a, w.\`Tuple(a UInt32, b UInt32)\`.b FROM test_rename_escape"

$CH -q "DROP TABLE test_rename_escape"
