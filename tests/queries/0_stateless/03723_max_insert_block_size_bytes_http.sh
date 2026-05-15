#!/usr/bin/env bash
# Tags: no-async-insert
# no-async-insert: Test expects new part for each insert

# Tests how input format parsers form blocks based on min/max thresholds
# 1. Creates 4 test tables
# 2. Inserts 8 rows (TSV format) via HTTP with different block formation thresholds:
#    - Test 1: max_insert_block_size_bytes=8 (emit block when reaching 8 bytes)
#    - Test 2: min_insert_block_size_rows=2 AND min_insert_block_size_bytes=16 (emit when both met)
#    - Test 3: min_insert_block_size_rows=4 (emit when 4 rows accumulated)
#    - Test 4: min_insert_block_size_bytes=32 (emit when 32 bytes accumulated)
# 3. Verifies the number of parts created to confirm the new isEnoughSize() logic:
#    - min thresholds use AND: both rows AND bytes must be satisfied
#    - max thresholds use OR: either rows OR bytes triggers block emission

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP table IF EXISTS test_max_insert_bytes_sh;" 
$CLICKHOUSE_CLIENT -q "DROP table IF EXISTS test_min_insert_rows_bytes_sh;"
$CLICKHOUSE_CLIENT -q "DROP table IF EXISTS test_min_insert_rows_sh;"
$CLICKHOUSE_CLIENT -q "DROP table IF EXISTS test_min_insert_bytes_sh;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_max_insert_bytes_sh (id UInt64) Engine = MergeTree() Order by id;" 
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_min_insert_rows_bytes_sh (id UInt64) Engine = MergeTree() Order by id;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_min_insert_rows_sh (id UInt64) Engine = MergeTree() Order by id;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_min_insert_bytes_sh (id UInt64) Engine = MergeTree() Order by id;"

echo -ne '1\n2\n3\n4\n5\n6\n7\n8' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_max_insert_bytes_sh+FORMAT+TSV&max_insert_block_size_bytes=8&max_insert_block_size=1000000&min_insert_block_size_rows=0&min_insert_block_size_bytes=0" --data-binary @-
echo -ne '1\n2\n3\n4\n5\n6\n7\n8' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_min_insert_rows_bytes_sh+FORMAT+TSV&max_insert_block_size_bytes=1000000&max_insert_block_size=1000000&min_insert_block_size_rows=2&min_insert_block_size_bytes=16" --data-binary @-
echo -ne '1\n2\n3\n4\n5\n6\n7\n8' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_min_insert_rows_sh+FORMAT+TSV&max_insert_block_size_bytes=1000000&max_insert_block_size=1000000&min_insert_block_size_rows=4&min_insert_block_size_bytes=0" --data-binary @-
echo -ne '1\n2\n3\n4\n5\n6\n7\n8' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_min_insert_bytes_sh+FORMAT+TSV&max_insert_block_size_bytes=1000000&max_insert_block_size=1000000&min_insert_block_size_rows=0&min_insert_block_size_bytes=32" --data-binary @-

# Wait for all HTTP insert queries to appear in query_log.
# There is a race between HTTP response being sent and the query_log entry being written.
for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, part_log"
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'INSERT INTO test_m%_insert_%_sh FORMAT TSV%' AND type = 'QueryFinish'")
    [ "$count" -ge 4 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.part_log WHERE table = 'test_max_insert_bytes_sh' AND event_type = 'NewPart' AND (query_id = (SELECT argMax(query_id, event_time) FROM system.query_log WHERE query LIKE '%INSERT INTO test_max_insert_bytes_sh FORMAT T%' AND current_database = currentDatabase()))"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.part_log WHERE table = 'test_min_insert_rows_bytes_sh' AND event_type = 'NewPart' AND (query_id = (SELECT argMax(query_id, event_time) FROM system.query_log WHERE query LIKE '%INSERT INTO test_min_insert_rows_bytes_sh FORMAT TSV%' AND current_database = currentDatabase()))"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.part_log WHERE table = 'test_min_insert_rows_sh' AND event_type = 'NewPart' AND (query_id = (SELECT argMax(query_id, event_time) FROM system.query_log WHERE query LIKE '%INSERT INTO test_min_insert_rows_sh FORMAT TSV%' AND current_database = currentDatabase()))"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.part_log WHERE table = 'test_min_insert_bytes_sh' AND event_type = 'NewPart' AND (query_id = (SELECT argMax(query_id, event_time) FROM system.query_log WHERE query LIKE '%INSERT INTO test_min_insert_bytes_sh FORMAT TSV%' AND current_database = currentDatabase()))"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_max_insert_bytes_sh"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_min_insert_rows_bytes_sh"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_min_insert_rows_sh"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_min_insert_bytes_sh"