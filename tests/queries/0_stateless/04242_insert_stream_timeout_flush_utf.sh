#!/usr/bin/env bash
# Tags: no-async-insert, no-fasttest
# no-fasttest: Too slow for fast test (~14s), covered by regular stateless runs.
# no-async-insert: Test expects new part for each time interval

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout_utf"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_timeout_utf (id UInt64, data String) ENGINE MergeTree ORDER BY id"

# We feed UTF-16LE data with BOM.
# The python helper will generate batches of JSONEachRow, encode to UTF-16LE with BOM, and stream with a sleep.
python3 -c '
import sys
import time

# Prepend BOM for UTF-16LE
sys.stdout.buffer.write(b"\xff\xfe")

def write_batch(iteration):
    batch = ""
    for i in range(1, 41):
        batch += f"{{\"id\":{(iteration*100) + i},\"data\":\"batch_{iteration}\"}}\n"
    sys.stdout.buffer.write(batch.encode("utf-16-le"))
    sys.stdout.flush()

def write_trigger(iteration):
    trigger = f"{{\"id\":{(iteration*100) + 99},\"data\":\"trigger_{iteration}\"}}\n"
    sys.stdout.buffer.write(trigger.encode("utf-16-le"))
    sys.stdout.flush()

for iteration in [1, 2]:
    write_batch(iteration)
    time.sleep(6)
    write_trigger(iteration)
' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_timeout_utf FORMAT JSONEachRow" \
    --max_insert_block_size=1000 \
    --input_format_connection_handling=1 \
    --input_format_max_block_wait_ms=2000 \
    --min_insert_block_size_bytes=0 \
    --min_insert_block_size_rows=0

sleep 1
record_count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_insert_timeout_utf")
echo "Total records inserted: ${record_count}"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, part_log;"

parts_count=$(${CLICKHOUSE_CLIENT} --query "
SELECT count(*) 
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND table = 'test_insert_timeout_utf'
  AND event_type = 'NewPart'
  AND query_id = (
        SELECT argMax(query_id, event_time)
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND query LIKE '%INSERT INTO test_insert_timeout_utf%'
          AND current_database = currentDatabase()
    )
")

echo "Number of parts created: ${parts_count}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout_utf"
