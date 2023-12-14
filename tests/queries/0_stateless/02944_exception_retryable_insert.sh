#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS retry_inserts;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE retry_inserts (number UInt64) ENGINE = ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/retry_inserts', '1') ORDER BY number;"

function run_and_grep()
{
  $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "$1" -v |& grep "X-ClickHouse-Exception" | sed 's/Exception/EXC/g'
  $CLICKHOUSE_CLIENT -q "Select count() from retry_inserts"
}


echo "ZK failure"
run_and_grep "INSERT INTO retry_inserts SELECT * FROM numbers(100) SETTINGS insert_keeper_fault_injection_probability=1"

echo "Timeout"
run_and_grep "INSERT INTO retry_inserts SELECT sleepEachRow(0.1) FROM numbers(30) SETTINGS max_execution_time = 0.5, function_sleep_max_microseconds_per_block=10000000000, max_block_size=1000"
