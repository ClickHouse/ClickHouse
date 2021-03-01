#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS defaults"
$CLICKHOUSE_CLIENT --query="CREATE TABLE defaults (n UInt8, s String DEFAULT 'hello') ENGINE = Memory"
echo '{"n": 1} {"n": 2, "s":"world"}' | $CLICKHOUSE_CLIENT --max_insert_block_size=1 --query="INSERT INTO defaults FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="SELECT * FROM defaults ORDER BY n"
$CLICKHOUSE_CLIENT --query="DROP TABLE defaults"
