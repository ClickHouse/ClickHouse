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

# Verify row placement per part to ensure timeout flush occurred correctly
${CLICKHOUSE_CLIENT} --query "SELECT min(id), max(id), count() FROM test_insert_timeout_utf GROUP BY _part ORDER BY min(id)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout_utf"
