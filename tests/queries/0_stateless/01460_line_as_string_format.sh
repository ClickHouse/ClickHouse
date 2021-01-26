#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS line_as_string1";
$CLICKHOUSE_CLIENT --query="CREATE TABLE line_as_string1(field String) ENGINE = Memory";

echo '"id" : 1,
"date" : "01.01.2020",
"string" : "123{{{\"\\",
"array" : [1, 2, 3],

Finally implement this new feature.' | $CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string1 FORMAT LineAsString";

$CLICKHOUSE_CLIENT --query="SELECT * FROM line_as_string1";
$CLICKHOUSE_CLIENT --query="DROP TABLE line_as_string1"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS line_as_string2";
$CLICKHOUSE_CLIENT --query="create table line_as_string2(
    a UInt64 default 42, 
    b String materialized toString(a),
    c String
) engine=MergeTree() order by tuple();";

$CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string2(c) values ('ClickHouse')";

echo 'ClickHouse is a `fast` #open-source# (OLAP) 'database' "management" :system:' | $CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string2(c) FORMAT LineAsString";

$CLICKHOUSE_CLIENT --query="SELECT * FROM line_as_string2 order by c";
$CLICKHOUSE_CLIENT --query="DROP TABLE line_as_string2"

$CLICKHOUSE_CLIENT --query="select repeat('aaa',50) from numbers(1000000)" > "${CLICKHOUSE_TMP}"/data1
$CLICKHOUSE_CLIENT --query="CREATE TABLE line_as_string3(field String) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string3 FORMAT LineAsString" < "${CLICKHOUSE_TMP}"/data1
$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM line_as_string3";
$CLICKHOUSE_CLIENT --query="DROP TABLE line_as_string3"

$CLICKHOUSE_CLIENT --query="select randomString(50000) FROM numbers(1000)" > "${CLICKHOUSE_TMP}"/data2
$CLICKHOUSE_CLIENT --query="CREATE TABLE line_as_string4(field String) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string4 FORMAT LineAsString" < "${CLICKHOUSE_TMP}"/data2
$CLICKHOUSE_CLIENT --query="SELECT count(*) FROM line_as_string4";
$CLICKHOUSE_CLIENT --query="DROP TABLE line_as_string4"
