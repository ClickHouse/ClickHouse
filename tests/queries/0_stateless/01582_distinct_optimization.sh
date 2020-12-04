#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query="CREATE TABLE test_local (a String, b Int) Engine=TinyLog"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_local VALUES('a', 0), ('a', 1), ('b', 0)"
echo "----"
$CLICKHOUSE_CLIENT --query="EXPLAIN PIPELINE SELECT DISTINCT b FROM (SELECT b FROM remote('127.0.0.{1,2}', '$CLICKHOUSE_DATABASE', test_local) GROUP BY a, b)" | grep -o "DistinctTransform" || true
echo "----"
$CLICKHOUSE_CLIENT --query="EXPLAIN PIPELINE SELECT DISTINCT a, b, b + 1 FROM (SELECT a, b FROM remote('127.0.0.{1,2}', '$CLICKHOUSE_DATABASE', test_local) GROUP BY a, b)" | grep -o "DistinctTransform" || true
