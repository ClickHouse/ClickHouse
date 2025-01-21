#!/usr/bin/env bash
# Tags: long, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS testing_memory;"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE testing_memory (a UInt64, b String) ENGINE = MergeTree ORDER BY ();
"

settings="
    max_insert_threads=500, max_memory_usage = 4e8, max_rows_to_read = 300000000, max_execution_time=100
"

# Insert initial data with throttling enabled. I've changed max_execution time because test can take too much time to run if the server has memory shortage
insert_status=$($CLICKHOUSE_CLIENT --query="
INSERT INTO testing_memory
SELECT number, toString(number) FROM system.numbers LIMIT 200000000
SETTINGS $settings, enable_memory_based_pipeline_throttling = 1;
" 2>&1) || true

if [[ $insert_status == *"TIMEOUT_EXCEEDED"* || $insert_status == "" ]]; then
    echo "Query succeeded without errors or got TIMEOUT_EXCEEDED."
else
    echo "Unexpected behavior: $insert_status"
    exit 1
fi

# Attempt to insert more data with throttling disabled
insert_status=$($CLICKHOUSE_CLIENT --query="
INSERT INTO testing_memory 
SELECT number, toString(number) FROM system.numbers LIMIT 200000000
SETTINGS $settings, enable_memory_based_pipeline_throttling = 0;
" 2>&1) || true

if [[ $insert_status == *"MEMORY_LIMIT_EXCEEDED"* ]]; then
    echo "Expected MEMORY_LIMIT_EXCEEDED error occurred."
else
    echo "Unexpected behavior: $insert_status"
    exit 1
fi

# Cleanup
$CLICKHOUSE_CLIENT --query="DROP TABLE testing_memory;"
