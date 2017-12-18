#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.csv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.csv (s String, n UInt64, d Date) ENGINE = Memory";

echo '"Hello, world", 123, "2016-01-01"
"Hello, ""world""", "456", 2016-01-02,
Hello "world", 789 ,2016-01-03
"Hello
 world", 100, 2016-01-04,' | $CLICKHOUSE_CLIENT --query="INSERT INTO test.csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.csv ORDER BY d";
$CLICKHOUSE_CLIENT --query="DROP TABLE test.csv";

$CLICKHOUSE_CLIENT --query="CREATE TABLE test.csv (t DateTime, s String) ENGINE = Memory";

echo '"2016-01-01 01:02:03","1"
2016-01-02 01:02:03, "2"
1502792101,"3"
99999,"4"' | $CLICKHOUSE_CLIENT --query="INSERT INTO test.csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.csv ORDER BY s";
$CLICKHOUSE_CLIENT --query="DROP TABLE test.csv";
