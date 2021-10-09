#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS defaults"
$CLICKHOUSE_CLIENT --query="CREATE TABLE defaults (x UInt32, y UInt32, a DEFAULT x + y, b Float32 DEFAULT round(log(1 + x + y), 5), c UInt32 DEFAULT 42, e MATERIALIZED x + y, f ALIAS x + y) ENGINE = Memory"

echo -ne '{"x":1, "y":1}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20defaults%20FORMAT%20JSONEachRow%20SETTINGS%20input_format_defaults_for_omitted_fields=1" --data-binary @-
echo -ne '{"x":2, "y":2, "c":2}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+defaults+FORMAT+JSONEachRow+SETTINGS+input_format_defaults_for_omitted_fields=1" --data-binary @-
echo -ne '{"x":3, "y":3, "a":3, "b":3, "c":3}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&database=${CLICKHOUSE_DATABASE}&query=INSERT+INTO+defaults+FORMAT+JSONEachRow+SETTINGS+input_format_defaults_for_omitted_fields=1" --data-binary @-
echo -ne '{"x":4} {"y":5, "c":5} {"a":6, "b":6, "c":6}\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&database=${CLICKHOUSE_DATABASE}&query=INSERT+INTO+defaults+FORMAT+JSONEachRow+SETTINGS+input_format_defaults_for_omitted_fields=1" --data-binary @-

$CLICKHOUSE_CLIENT --query="SELECT * FROM defaults ORDER BY x, y FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="DROP TABLE defaults"
