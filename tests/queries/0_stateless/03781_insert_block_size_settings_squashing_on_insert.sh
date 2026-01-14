#!/usr/bin/env bash
# Tags: no-async-insert
# Test cases:
#   1. max_insert_block_size_rows splits by row threshold
#   2. max_insert_block_size_bytes splits by byte threshold  
#   3. min thresholds squash small blocks together
#   4. Data integrity verification

# set -e

# CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# # shellcheck source=../shell_config.sh
# . "$CUR_DIR"/../shell_config.sh

CH="/home/ubuntu/ClickHouse/build-asan/programs/clickhouse"

$CH client -q "DROP TABLE IF EXISTS test_native_max_rows"
$CH client -q "DROP TABLE IF EXISTS test_native_max_bytes"
$CH client -q "DROP TABLE IF EXISTS test_native_min_squash"
$CH client -q "DROP TABLE IF EXISTS test_parquet_max_rows"

$CH client -q "CREATE TABLE test_native_max_rows (id UInt64) ENGINE = MergeTree() ORDER BY id"
$CH client -q "CREATE TABLE test_native_max_bytes (id UInt64) ENGINE = MergeTree() ORDER BY id"
$CH client -q "CREATE TABLE test_native_min_squash (id UInt64) ENGINE = MergeTree() ORDER BY id"
$CH client -q "CREATE TABLE test_parquet_max_rows (id UInt64) ENGINE = MergeTree() ORDER BY id"

$CH client -q "SELECT number FROM numbers(100) FORMAT Native" | \
$CH client \
    --max_insert_block_size_rows=1 \
    --max_insert_block_size_bytes=0 \
    --min_insert_block_size_rows=1 \
    --min_insert_block_size_bytes=0 \
    --use_strict_insert_block_limits=1 \
    -q "INSERT INTO test_native_max_rows FORMAT Native"

$CH client -q "SELECT number FROM numbers(100) FORMAT Native" | \
$CH client \
    --max_insert_block_size_rows=0 \
    --max_insert_block_size_bytes=163 \
    --min_insert_block_size_rows=0 \
    --min_insert_block_size_bytes=0 \
    --use_strict_insert_block_limits=1 \
    -q "INSERT INTO test_native_max_bytes FORMAT Native"

$CH client --max_block_size=10 -q "SELECT number FROM numbers(100) FORMAT Native" | \
$CH client \
    --max_insert_block_size_rows=0 \
    --max_insert_block_size_bytes=0 \
    --min_insert_block_size_rows=33 \
    --min_insert_block_size_bytes=8 \
    --use_strict_insert_block_limits=1 \
    -q "INSERT INTO test_native_min_squash FORMAT Native"


$CH client -q "SELECT number FROM numbers(100) FORMAT Parquet" | \
$CH client \
    --max_insert_block_size_rows=32 \
    --max_insert_block_size_bytes=0 \
    --min_insert_block_size_rows=0 \
    --min_insert_block_size_bytes=0 \
    --use_strict_insert_block_limits=1 \
    -q "INSERT INTO test_parquet_max_rows FORMAT Parquet"

$CH client -q "SYSTEM FLUSH LOGS query_log, part_log;"

# Test 1: Expect 4 parts (ceil(100 / 1) = 100)
$CH client -q "
SELECT count()
FROM system.part_log
WHERE table = 'test_native_max_rows'
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND (query_id = (
    SELECT argMax(query_id, event_time) 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_native_max_rows FORMAT Native%' 
    AND current_database = currentDatabase() 
));
"

# Test 2: Expect 5 parts (ceil(800 bytes / 163) = 5)
$CH client -q "
SELECT count()
FROM system.part_log
WHERE table = 'test_native_max_bytes'
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND (query_id = (
    SELECT argMax(query_id, event_time) 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_native_max_bytes FORMAT Native%' 
    AND current_database = currentDatabase() 
));
"

# Test 3: Expect 3 parts (100 rows, min 33 rows -> ceil(100/33) = 3)
$CH client -q "
SELECT count()
FROM system.part_log
WHERE table = 'test_native_min_squash'
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND (query_id = (
    SELECT argMax(query_id, event_time) 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_native_min_squash FORMAT Native%' 
    AND current_database = currentDatabase() 
));
"

# Test 4: Expect 4 parts (ceil(100 / 32) = 4)
$CH client -q "
SELECT count()
FROM system.part_log
WHERE table = 'test_parquet_max_rows'
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND (query_id = (
    SELECT argMax(query_id, event_time) 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_parquet_max_rows FORMAT Parquet%' 
    AND current_database = currentDatabase() 
));
"

$CH client -q "SELECT count() FROM test_native_max_rows"
$CH client -q "SELECT count() FROM test_native_max_bytes"
$CH client -q "SELECT count() FROM test_native_min_squash"
$CH client -q "SELECT count() FROM test_parquet_max_rows"

$CH client -q "DROP TABLE test_native_max_rows"
$CH client -q "DROP TABLE test_native_max_bytes"
$CH client -q "DROP TABLE test_native_min_squash"
$CH client -q "DROP TABLE test_parquet_max_rows"