#!/usr/bin/env bash
# Tags: long, no-parallel-replicas

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS rp_04545 ON t_streaming_rp"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_streaming_rp"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_streaming_rp (a UInt32, b UInt32)
    ENGINE = MergeTree PARTITION BY a ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_streaming_rp SELECT number, 9000 FROM numbers(10)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_streaming_rp SELECT number % 10, number % 5000 FROM numbers(10000)"

$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY rp_04545 ON t_streaming_rp FOR SELECT USING b < 5000 TO ALL"

for _ in {1..30}; do
    $CLICKHOUSE_CLIENT --enable_streaming_queries=1 --query_plan_remove_unused_columns=1 --max_threads=4 --max_threads_min_free_memory_per_thread=0 --max_execution_time=20 \
        -q "SELECT throwIf(max(b) >= 5000 OR count() = 0) FROM (SELECT b FROM t_streaming_rp STREAM LIMIT 400) FORMAT Null"
done

echo "ok"

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY rp_04545 ON t_streaming_rp"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_streaming_rp"
