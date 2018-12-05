#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.test(num1 UInt64, num2 UInt64) ENGINE = Log"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.test WITH number * 2 AS num2 SELECT toString(number) || ',' || toString(num2) FROM numbers(10) FORMAT CSV"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.test SELECT * FROM file('$CURDIR/insert_select_format.csv') FORMAT CSV"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.test SELECT arrayStringConcat(['123', '456'], ',') FORMAT CSV"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.test"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.test"
