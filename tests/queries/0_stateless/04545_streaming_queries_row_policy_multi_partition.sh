#!/usr/bin/env bash
# Tags: long, no-parallel-replicas

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

$STREAMING_CLIENT -q "DROP ROW POLICY IF EXISTS rp_04545 ON t_streaming_rp"
$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_rp"

$STREAMING_CLIENT -q "
    CREATE TABLE t_streaming_rp (a UInt32, b UInt32)
    ENGINE = MergeTree PARTITION BY a ORDER BY a
    SETTINGS $STREAMING_TABLE_SETTINGS"

$STREAMING_CLIENT -q "INSERT INTO t_streaming_rp SELECT number, 9000 FROM numbers(10)"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_rp SELECT number % 10, number % 5000 FROM numbers(10000)"

$STREAMING_CLIENT -q "CREATE ROW POLICY rp_04545 ON t_streaming_rp FOR SELECT USING b < 5000 TO ALL"

for _ in {1..30}; do
    $STREAMING_CLIENT -q "
        SELECT throwIf(max(b) >= 5000 OR count() = 0) FROM (SELECT b FROM t_streaming_rp STREAM LIMIT 400)
        SETTINGS query_plan_remove_unused_columns = 1, max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_execution_time = 20
        FORMAT Null"
done

echo "ok"

$STREAMING_CLIENT -q "DROP ROW POLICY rp_04545 ON t_streaming_rp"
$STREAMING_CLIENT -q "DROP TABLE t_streaming_rp"
