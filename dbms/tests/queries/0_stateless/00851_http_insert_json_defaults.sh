#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.defaults"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.defaults (x UInt32, y UInt32, a DEFAULT x + y, b Float32 DEFAULT log(1 + x + y), c UInt32 DEFAULT 42, e MATERIALIZED x + y, f ALIAS x + y) ENGINE = Memory"

echo -ne '{"x":1, "y":1}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT%20INTO%20test.defaults%20FORMAT%20JSONEachRow%20SETTINGS%20insert_sample_with_metadata=1" --data-binary @-
echo -ne '{"x":2, "y":2, "c":2}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+test.defaults+FORMAT+JSONEachRow+SETTINGS+insert_sample_with_metadata=1" --data-binary @-
echo -ne '{"x":3, "y":3, "a":3, "b":3, "c":3}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+test.defaults+FORMAT+JSONEachRow+SETTINGS+insert_sample_with_metadata=1" --data-binary @-
echo -ne '{"x":4} {"y":5, "c":5} {"a":6, "b":6, "c":6}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+test.defaults+FORMAT+JSONEachRow+SETTINGS+insert_sample_with_metadata=1" --data-binary @-

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.defaults ORDER BY x, y FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="DROP TABLE test.defaults"
