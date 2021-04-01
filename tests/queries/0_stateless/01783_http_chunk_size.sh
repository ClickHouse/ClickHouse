#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "DROP TABLE IF EXISTS table" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&session_id=01783" -d @-
echo "CREATE TABLE table (a String) ENGINE Memory()" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&session_id=01783" -d @-

# NOTE: suppose that curl sends everything in a single chunk - there are no options to force the chunk-size.
echo "SET max_query_size=44" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&session_id=01783" -d @-
echo -ne "INSERT INTO TABLE table FORMAT TabSeparated 1234567890 1234567890 1234567890 1234567890\n" | ${CLICKHOUSE_CURL} -H "Transfer-Encoding: chunked" -sS "${CLICKHOUSE_URL}&session_id=01783" --data-binary @-

echo "SELECT * from table" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&session_id=01783" -d @-
echo "DROP TABLE table" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&session_id=01783" -d @-
