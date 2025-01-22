#!/usr/bin/env bash
# Tags: long, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS testing_memory;"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE testing_memory (a UInt64, b String) ENGINE = MergeTree ORDER BY ();
"

settings="max_insert_threads=32, max_memory_usage=1e9, max_execution_time=60"

# 1) Insert with memory-based pipeline throttling enabled.
insert_status=$($CLICKHOUSE_CLIENT --query="
INSERT INTO testing_memory
SELECT number, replicate('x', range(1, 50)) FROM numbers(600000)
SETTINGS $settings, enable_memory_based_pipeline_throttling=1
" 2>&1) || true

# We accept either successful completion (empty stderr) or a TIMEOUT_EXCEEDED.
if [[ "$insert_status" == *"TIMEOUT_EXCEEDED"* || -z "$insert_status" ]]; then
    echo "Query succeeded without errors or got TIMEOUT_EXCEEDED."
else
    echo "Unexpected behavior: $insert_status"
    exit 1
fi

# 2) Insert with the same settings but throttling disabled; we expect MEMORY_LIMIT_EXCEEDED.
insert_status=$($CLICKHOUSE_CLIENT --query="
INSERT INTO testing_memory
SELECT number, replicate('x', range(1, 129)) FROM numbers(600000)
SETTINGS $settings, enable_memory_based_pipeline_throttling=0
" 2>&1) || true

if [[ "$insert_status" == *"MEMORY_LIMIT_EXCEEDED"* ]]; then
    echo "Expected MEMORY_LIMIT_EXCEEDED error occurred."
else
    echo "Unexpected behavior: $insert_status"
    exit 1
fi

# Cleanup
$CLICKHOUSE_CLIENT --query="DROP TABLE testing_memory;"
