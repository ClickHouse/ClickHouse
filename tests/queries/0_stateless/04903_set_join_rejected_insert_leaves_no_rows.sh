#!/usr/bin/env bash
# A persistent `Set` / `Join` stages every block into `<data path>/tmp/<id>.bin`, and only `onFinish`
# promotes that file into the table directory. `restore` scans the table directory, not `tmp`, so a
# staged file left behind by a failed `INSERT` would never be read again and its data would stay on
# disk forever. Check that a failure after the staged file was opened leaves nothing behind.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Set` and `Join` report a disk-relative data path, so prepend the path of the disk they use.
disk_path=$($CLICKHOUSE_CLIENT --query "SELECT path FROM system.disks WHERE name = 'default'")

function staged_files_left()
{
    local table_path
    table_path=$($CLICKHOUSE_CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE name = '$1' AND database = currentDatabase()")
    find "${disk_path}${table_path}tmp" -name '*.bin' 2>/dev/null | wc -l
}

echo "-- Set"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS staged_set"
$CLICKHOUSE_CLIENT --query "CREATE TABLE staged_set (k UInt64) ENGINE = Set SETTINGS persistent = 1"

# One thread and small blocks, so the sink stages the first blocks - and thus opens the staged file -
# before the source throws on a later one.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO staged_set SELECT number + throwIf(number = 4096, 'Injected failure') FROM numbers(8192)
    SETTINGS max_threads = 1, max_block_size = 512, max_insert_block_size = 512,
             min_insert_block_size_rows = 512, min_insert_block_size_bytes = 0
" 2>&1 | grep -o 'Injected failure' | head -n 1
echo "staged files left behind: $(staged_files_left staged_set)"
echo "matching rows immediately after failed insert:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(8192) WHERE number IN staged_set"

# The table is still usable, and only the successful insert survives a reattach.
$CLICKHOUSE_CLIENT --query "INSERT INTO staged_set VALUES (1)"
$CLICKHOUSE_CLIENT --query "DETACH TABLE staged_set"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE staged_set"
echo "matching rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM numbers(8192) WHERE number IN staged_set"
$CLICKHOUSE_CLIENT --query "DROP TABLE staged_set"

echo "-- Join"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS staged_join"
$CLICKHOUSE_CLIENT --query "CREATE TABLE staged_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 1"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO staged_join SELECT number, number + throwIf(number = 4096, 'Injected failure') FROM numbers(8192)
    SETTINGS max_threads = 1, max_block_size = 512, max_insert_block_size = 512,
             min_insert_block_size_rows = 512, min_insert_block_size_bytes = 0
" 2>&1 | grep -o 'Injected failure' | head -n 1
echo "staged files left behind: $(staged_files_left staged_join)"
echo "rows immediately after failed insert:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM staged_join"

$CLICKHOUSE_CLIENT --query "INSERT INTO staged_join VALUES (1, 10)"
$CLICKHOUSE_CLIENT --query "DETACH TABLE staged_join"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE staged_join"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM staged_join"
$CLICKHOUSE_CLIENT --query "DROP TABLE staged_join"

echo "-- Join limit"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS limited_join"
$CLICKHOUSE_CLIENT --query "CREATE TABLE limited_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 1, max_rows_in_join = 1, join_overflow_mode = 'throw'"

# `HashJoin::addBlockToJoin` used to add the block before checking its limits. The persistent
# sink must restore the previous state when this publish step throws.
$CLICKHOUSE_CLIENT --query "INSERT INTO limited_join VALUES (1, 10), (2, 20)" 2>&1 | grep -o 'SET_SIZE_LIMIT_EXCEEDED' | head -n 1
echo "rows immediately after rejected insert:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM limited_join"
$CLICKHOUSE_CLIENT --query "DETACH TABLE limited_join"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE limited_join"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM limited_join"
$CLICKHOUSE_CLIENT --query "DROP TABLE limited_join"
