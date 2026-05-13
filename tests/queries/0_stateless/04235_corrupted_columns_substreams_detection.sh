#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-object-storage

# Test that corrupted columns_substreams.txt (from a historical rename bug) is detected
# and safely discarded at load time, allowing the part to work correctly without it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_corrupted_substreams"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_corrupted_substreams
    (
        id UInt64,
        arr Array(UInt32)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0,
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_corrupted_substreams SELECT number, [number, number + 1] FROM numbers(10)"

echo "Data before corruption:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), sum(length(arr)) FROM t_corrupted_substreams"

# Get the data path of the active part.
DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_corrupted_substreams' AND active")

# Detach the table so we can modify files on disk.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_corrupted_substreams"

# Corrupt columns_substreams.txt by writing substream names that simulate the rename bug:
# substream names like "arrwrong" instead of "arr" or "arr.size0".
cat > "${DATA_PATH}columns_substreams.txt" << 'EOF'
columns substreams version: 1
2 columns:
1 substreams for column `id`:
	id
1 substreams for column `arr`:
	arrwrongprefix
EOF

# Attach the table - this triggers loadColumnsSubstreams which should detect the corruption,
# log a warning, and discard the corrupted data.
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_corrupted_substreams" 2>/dev/null

echo "Data after attach with corrupted file:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), sum(length(arr)) FROM t_corrupted_substreams"

# CHECK TABLE should also work (falls back to enumerateStreams since columns_substreams was discarded).
echo "CHECK TABLE result:"
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_corrupted_substreams SETTINGS check_query_single_value_result = 1"

# DETACH/ATTACH partition should also work.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_corrupted_substreams DETACH PARTITION tuple()"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_corrupted_substreams ATTACH PARTITION tuple()" 2>/dev/null

echo "Data after partition reattach:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), sum(length(arr)) FROM t_corrupted_substreams"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_corrupted_substreams"
