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
