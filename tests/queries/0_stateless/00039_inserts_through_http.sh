#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'DROP TABLE IF EXISTS long_insert' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'CREATE TABLE long_insert (str String) ENGINE = Memory' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
for string_size in 1 10 100 1000 10000 100000 1000000; do
    for letter in {a..z}; do rep "$letter" "$string_size"; echo; done | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&query=INSERT+INTO+long_insert+FORMAT+TabSeparated" --data-binary @-
    echo 'SELECT substring(str, 1, 1) AS c, length(str) AS l FROM long_insert ORDER BY c, l' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
done

echo 'DROP TABLE long_insert' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
