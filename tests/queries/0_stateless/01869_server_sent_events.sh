#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "select max(*) from numbers(1000000) group by (number / 100000)" | "${CLICKHOUSE_CURL}/stream" @-

echo "select max(*) from numbers(1000000) group by (number / 100000) format JSONEachRow" | "${CLICKHOUSE_URL}/stream" @- 

echo "with totals select max(*) from numbers(1000000) group by (number / 100000)" | "${CLICKHOUSE_CURL}/stream" @-

echo "with extremes select max(*) from numbers(1000000) group by (number / 100000)" | "${CLICKHOUSE_CURL}/stream" @-

# incorrect query, error should be sent
echo "selec max(*) from numbers(1000000) group by (number / 100000)" | "${CLICKHOUSE_CURL}/stream" @-


URL="${CLICKHOUSE_URL}/stream&session_id=id_${CLICKHOUSE_DATABASE}"

echo "DROP TABLE IF EXISTS table" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-
echo "CREATE TABLE table (a String) ENGINE Memory()" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-

echo -ne "INSERT INTO TABLE table FORMAT TabSeparated 1234567890 1234567890 1234567890 1234567890\n" | ${CLICKHOUSE_CURL} -sS "${URL}" --data-binary @-

echo "SELECT * from table" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-
echo "DROP TABLE table" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-
