#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT number FROM numbers(10)" > "${CLICKHOUSE_TMP}/numbers.jsonl.gz"
$CLICKHOUSE_LOCAL --query "SELECT * FROM table" < "${CLICKHOUSE_TMP}/numbers.jsonl.gz"
$CLICKHOUSE_LOCAL --query "SELECT * FROM table" < "${CLICKHOUSE_TMP}/numbers.jsonl.gz" > "${CLICKHOUSE_TMP}/numbers.csv.bz2"
$CLICKHOUSE_LOCAL --copy < "${CLICKHOUSE_TMP}/numbers.csv.bz2" > "${CLICKHOUSE_TMP}/numbers.parquet"
$CLICKHOUSE_LOCAL --copy < "${CLICKHOUSE_TMP}/numbers.parquet"

echo '---'

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test (number UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO test FORMAT JSONLines" < "${CLICKHOUSE_TMP}/numbers.jsonl.gz"
$CLICKHOUSE_CLIENT --query "INSERT INTO test FORMAT CSV" < "${CLICKHOUSE_TMP}/numbers.csv.bz2"
$CLICKHOUSE_CLIENT --query "INSERT INTO test SELECT c1 FROM input() FORMAT Parquet" < "${CLICKHOUSE_TMP}/numbers.parquet"
$CLICKHOUSE_CLIENT --query "SELECT * FROM test"
$CLICKHOUSE_CLIENT --query "DROP TABLE test"
