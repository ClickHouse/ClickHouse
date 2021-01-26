#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# BOM can be parsed if TSV format has first column that cannot contain arbitrary binary data (such as integer)
# In contrast, BOM cannot be parsed if the first column in String as it can contain arbitrary binary data.

echo 'DROP TABLE IF EXISTS bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'CREATE TABLE bom (a UInt8, b UInt8, c UInt8) ENGINE = Memory' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne '1\t2\t3\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+TSV" --data-binary @-
echo -ne '\xEF\xBB\xBF4\t5\t6\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+bom+FORMAT+TSV" --data-binary @-
echo 'SELECT * FROM bom ORDER BY a' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo 'DROP TABLE bom' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
