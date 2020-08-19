#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS line_as_string";

$CLICKHOUSE_CLIENT --query="CREATE TABLE line_as_string(field String) ENGINE = Memory";

echo '"id" : 1,
"date" : "01.01.2020",
"string" : "123{{{\"\\",
"array" : [1, 2, 3],

Finally implement this new feature.' | $CLICKHOUSE_CLIENT --query="INSERT INTO line_as_string FORMAT LineAsString";

$CLICKHOUSE_CLIENT --query="SELECT * FROM line_as_string";
$CLICKHOUSE_CLIENT --query="DROP TABLE line_as_string"

