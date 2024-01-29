#!/usr/bin/env bash
# Tags: shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

opts=(
    "--allow_experimental_analyzer=1"
    "--allow_experimental_streaming=1"
    "--max_distributed_connections=1"
    "--prefer_localhost_replica=0"
)

# setup
$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_test;"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "DROP TABLE IF EXISTS t_streaming_dist;"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree() ORDER BY (a)"
$CLICKHOUSE_CLIENT "${opts[@]}" -q "CREATE TABLE t_streaming_dist as t_streaming_test engine=Distributed(test_cluster_two_shards, currentDatabase(), t_streaming_test, b);"

echo "=== Test Streaming read from shards ==="

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('started', 0)"

# start stream
read -r fifo_1 pid_1 < <(spawn $CLICKHOUSE_CLIENT "${opts[@]}" -q "SELECT a FROM t_streaming_dist STREAM")

# should read twice because of 2 shards
read_until "$fifo_1" "started"
read_until "$fifo_1" "started"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('from-shard-1', 0)"
read_until "$fifo_1" "from-shard-1"
read_until "$fifo_1" "from-shard-1"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_dist VALUES ('from-shard-1', 0)"
read_until "$fifo_1" "from-shard-1"
read_until "$fifo_1" "from-shard-1"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_test VALUES ('from-shard-2', 1)"
read_until "$fifo_1" "from-shard-2"
read_until "$fifo_1" "from-shard-2"

$CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO t_streaming_dist VALUES ('from-shard-2', 1)"
read_until "$fifo_1" "from-shard-2"
read_until "$fifo_1" "from-shard-2"

# stop reading by killing client job
cleanup "$fifo_1" "$pid_1"
