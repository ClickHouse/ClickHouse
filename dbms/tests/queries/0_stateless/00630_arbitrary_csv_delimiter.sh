#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.csv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.csv (s String, n UInt64, d Date) ENGINE = Memory";

echo '"Hello, world"| 123| "2016-01-01"
"Hello, ""world"""| "456"| 2016-01-02|
Hello "world"| 789 |2016-01-03
"Hello
 world"| 100| 2016-01-04|' | $CLICKHOUSE_CLIENT --format_csv_delimiter="|"  --query="INSERT INTO test.csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.csv ORDER BY d";

$CLICKHOUSE_CLIENT --query="DROP TABLE test.csv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.csv (s String, n UInt64, d Date) ENGINE = Memory";

echo '"Hello, world"; 123; "2016-01-01"
"Hello, ""world"""; "456"; 2016-01-02;
Hello "world"; 789 ;2016-01-03
"Hello
 world"; 100; 2016-01-04;' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_delimiter=';'; INSERT INTO test.csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.csv ORDER BY d";
$CLICKHOUSE_CLIENT --format_csv_delimiter=";" --query="SELECT * FROM test.csv ORDER BY d FORMAT CSV";
$CLICKHOUSE_CLIENT --format_csv_delimiter="/" --query="SELECT * FROM test.csv ORDER BY d FORMAT CSV";

$CLICKHOUSE_CLIENT --query="DROP TABLE test.csv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.csv (s1 String, s2 String) ENGINE = Memory";

echo 'abc,def;hello;
hello; world;
"hello ""world""";abc,def;' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_delimiter=';'; INSERT INTO test.csv FORMAT CSV";


$CLICKHOUSE_CLIENT --query="SELECT * FROM test.csv";

$CLICKHOUSE_CLIENT --query="DROP TABLE test.csv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.csv (s1 String, s2 String) ENGINE = Memory";

echo '"s1";"s2"
abc,def;hello;
hello; world;
"hello ""world""";abc,def;' | $CLICKHOUSE_CLIENT --multiquery --query="SET format_csv_delimiter=';'; INSERT INTO test.csv FORMAT CSVWithNames";

$CLICKHOUSE_CLIENT --format_csv_delimiter=";" --query="SELECT * FROM test.csv FORMAT CSV";
$CLICKHOUSE_CLIENT --format_csv_delimiter="," --query="SELECT * FROM test.csv FORMAT CSV";
$CLICKHOUSE_CLIENT --format_csv_delimiter="/" --query="SELECT * FROM test.csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="DROP TABLE test.csv";
