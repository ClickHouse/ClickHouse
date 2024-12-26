#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT number FROM numbers(10)" > numbers.jsonl.gz
$CLICKHOUSE_LOCAL --query "SELECT * FROM table" < numbers.jsonl.gz
$CLICKHOUSE_LOCAL --query "SELECT * FROM table" < numbers.jsonl.gz > numbers.csv.bz2
$CLICKHOUSE_LOCAL --copy < numbers.csv.bz2 > numbers.parquet
$CLICKHOUSE_LOCAL --copy < numbers.parquet

echo '---'

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test (number UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO test FORMAT JSONLines" < numbers.jsonl.gz
$CLICKHOUSE_CLIENT --query "INSERT INTO test FORMAT CSV" < numbers.csv.bz2
$CLICKHOUSE_CLIENT --query "INSERT INTO test SELECT c1 FROM input() FORMAT Parquet" < numbers.parquet
$CLICKHOUSE_CLIENT --query "SELECT * FROM test"
$CLICKHOUSE_CLIENT --query "DROP TABLE test"
