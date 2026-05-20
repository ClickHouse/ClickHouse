#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (x UInt64, s String) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst (x UInt64, s String) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query="INSERT INTO src SELECT number, toString(number) FROM numbers(10000)"

# MemoryAwareResize should appear in the pipeline
echo "--- throttle enabled ---"
$CLICKHOUSE_CLIENT --query="EXPLAIN PIPELINE INSERT INTO dst SELECT * FROM src SETTINGS enable_insert_memory_throttle = 1, max_insert_threads = 4, max_threads = 4" | grep -c "MemoryAwareResize"

# here plain Resize is used instead
echo "--- throttle disabled ---"
$CLICKHOUSE_CLIENT --query="EXPLAIN PIPELINE INSERT INTO dst SELECT * FROM src SETTINGS enable_insert_memory_throttle = 0, max_insert_threads = 4, max_threads = 4" | grep -c "MemoryAwareResize"

# all rows survive the throttled pipeline
echo "--- correctness ---"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst SELECT * FROM src SETTINGS enable_insert_memory_throttle = 1, max_insert_threads = 4, max_threads = 4"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM dst"
$CLICKHOUSE_CLIENT --query="SELECT sum(x) = (SELECT sum(x) FROM src) AS ok FROM dst"

$CLICKHOUSE_CLIENT --query="DROP TABLE src"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst"
