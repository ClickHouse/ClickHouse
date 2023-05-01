#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS format"
$CLICKHOUSE_CLIENT --query="CREATE TABLE format (s String, x FixedString(3)) ENGINE = Memory"

echo -ne '\tABC\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+format+FORMAT+TabSeparated" --data-binary @-
echo -ne 'INSERT INTO format FORMAT TabSeparated\n\tDEF\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO format FORMAT TabSeparated hello\tGHI\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO format FORMAT TabSeparated\r\n\tJKL\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO format FORMAT TabSeparated   \t\r\n\tMNO\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO format FORMAT TabSeparated\t\t\thello\tPQR\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-

$CLICKHOUSE_CLIENT --query="SELECT * FROM format ORDER BY s, x FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="DROP TABLE format"
