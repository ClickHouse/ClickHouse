#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.numbers";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.numbers (number UInt64) engine = MergeTree order by number";
$CLICKHOUSE_CLIENT --query="INSERT INTO test.numbers select * from system.numbers limit 10";

$CLICKHOUSE_CLIENT --query="SELECT number FROM test.numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM test.numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM test.numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM test.numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM test.numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM test.numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.numbers";