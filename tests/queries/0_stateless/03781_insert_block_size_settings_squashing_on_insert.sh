#!/usr/bin/env bash
# Tags: no-async-insert
# Test cases:
#   1. max_insert_block_size_rows splits by row threshold
#   2. Data integrity verification

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_native_max_rows"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_native_max_rows (id UInt64) ENGINE = MergeTree() ORDER BY id"

$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(100) FORMAT Native" | \
$CLICKHOUSE_CLIENT \
    --max_insert_block_size_rows=23 \
    --max_insert_block_size_bytes=0 \
    --min_insert_block_size_rows=0 \
    --min_insert_block_size_bytes=0 \
    --use_strict_insert_block_limits=1 \
    -q "INSERT INTO test_native_max_rows FORMAT Native"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log;"

# Test 1: Expect  parts (ceil(100 / 17) = 6)
$CLICKHOUSE_CLIENT -q "
SELECT count()
FROM system.part_log
WHERE table = 'test_native_max_rows'
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND event_time > (now() - 120);
"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_native_max_rows"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_native_max_rows"