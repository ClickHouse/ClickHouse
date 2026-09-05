#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '=== Test input_format_csv_empty_as_default'
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS csv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (s String, n UInt64 DEFAULT 1, d Date DEFAULT '2019-06-19') ENGINE = Memory";

printf '"Hello, world", 123, "2016-01-01"
"Hello, ""world""", "456", 2016-01-02,
Hello "world", 789 ,2016-01-03
"Hello
 world", 100, 2016-01-04,
 default,,
 default-eof,,' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_csv_empty_as_default=1 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY d, s";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";

echo '=== Test datetime'
$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (t DateTime('Asia/Istanbul'), s String) ENGINE = Memory";

echo '"2016-01-01 01:02:03","1"
2016-01-02 01:02:03, "2"
1502792101,"3"
99999,"4"' | $CLICKHOUSE_CLIENT --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY s";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";

echo '=== Test nullable datetime'
$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (t Nullable(DateTime('Asia/Istanbul')), s Nullable(String)) ENGINE = Memory";

echo 'NULL, NULL
"2016-01-01 01:02:03",NUL
"2016-01-02 01:02:03",Nhello' | $CLICKHOUSE_CLIENT --format_csv_null_representation='NULL' --input_format_csv_empty_as_default=1 --query="INSERT INTO csv FORMAT CSV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY s NULLS LAST";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";


echo '=== Test ignore extra columns'
$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (s String, n UInt64 DEFAULT 3, d String DEFAULT 'String4') ENGINE = Memory";

echo '"Hello", 1, "String1" 
"Hello", 2, "String2",
"Hello", 3, "String3", "2016-01-13"
"Hello", 4,        , "2016-01-14"
"Hello", 5, "String5", "2016-01-15", "2016-01-16"
"Hello", 6, "String6" , "line with a
break"' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_csv_empty_as_default=1 --input_format_csv_allow_variable_number_of_columns=1 --query="INSERT INTO csv FORMAT CSV";
$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY s, n";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";


echo '=== Test missing as default'
$CLICKHOUSE_CLIENT --query="CREATE TABLE csv (f1 String, f2 UInt64, f3 UInt256, f4 UInt64 Default 33, f5 Nullable(UInt64), f6 Nullable(UInt64) Default 55, f7 String DEFAULT 'Default') ENGINE = Memory";

echo '
,
"Hello"
"Hello",
"Hello", 1, 3, 2
"Hello",1,4,2,3,4,"String"
"Hello", 1, 4, 2, 3, 4, "String"
"Hello", 1, 5, 2, 3, 4, "String",'| $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_csv_allow_variable_number_of_columns=1 --query="INSERT INTO csv FORMAT CSV";
$CLICKHOUSE_CLIENT --query="SELECT * FROM csv ORDER BY f1, f2, f3, f4, f5 NULLS FIRST, f6, f7";
$CLICKHOUSE_CLIENT --query="DROP TABLE csv";
