#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

for typename in "UInt32" "UInt64" "Float64" "Float32"
do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS A;"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS B;"

    $CLICKHOUSE_CLIENT -q "CREATE TABLE A(k UInt32, t ${typename}, a Float64) ENGINE = MergeTree() ORDER BY (k, t);"
    $CLICKHOUSE_CLIENT -q "INSERT INTO A(k,t,a) VALUES (2,1,1),(2,3,3),(2,5,5);"

    $CLICKHOUSE_CLIENT -q "CREATE TABLE B(k UInt32, t ${typename}, b Float64) ENGINE = MergeTree() ORDER BY (k, t);"
    $CLICKHOUSE_CLIENT -q "INSERT INTO B(k,t,b) VALUES (2,3,3);"

    $CLICKHOUSE_CLIENT -q "SELECT k, t, a, b FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (k,t);"

    $CLICKHOUSE_CLIENT -q "DROP TABLE A;"
    $CLICKHOUSE_CLIENT -q "DROP TABLE B;"
done