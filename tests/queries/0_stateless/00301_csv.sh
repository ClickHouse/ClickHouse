#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS csv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (s String, n UInt64 DEFAULT 1, d Date DEFAULT '2019-06-19') ENGINE = Memory";

printf '"Hello, world", 123, "2016-01-01"
"Hello, ""world""", "456", 2016-01-02,
Hello "world", 789 ,2016-01-03
"Hello
 world", 100, 2016-01-04,
 default,,
 default-eof,,' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";

$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (t DateTime('Europe/Moscow'), s String) ENGINE = Memory";

echo '"2016-01-01 01:02:03","1"
2016-01-02 01:02:03, "2"
1502792101,"3"
99999,"4"' | $CLICKHOUSE_CLIENT --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY s";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";


$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (t Nullable(DateTime('Europe/Moscow')), s Nullable(String)) ENGINE = Memory";

echo 'NULL, NULL
"2016-01-01 01:02:03",NUL
"2016-01-02 01:02:03",Nhello' | $CLICKHOUSE_CLIENT --input_format_csv_unquoted_null_literal_as_null=1 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY s NULLS LAST";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";
