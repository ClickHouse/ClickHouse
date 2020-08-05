#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo 'DROP TABLE IF EXISTS bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'CREATE TABLE bom (a UInt8, b UInt8, c UInt8) ENGINE = Memory' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne '1,2,3\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+CSV" --data-binary @-
echo -ne '\xEF\xBB\xBF4,5,6\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+CSV" --data-binary @-
echo 'SELECT * FROM bom ORDER BY a' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'DROP TABLE bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
