#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.format"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.format (s String, x FixedString(3)) ENGINE = Memory"

echo -ne '\tABC\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+test.format+FORMAT+TabSeparated" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated\n\tDEF\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated hello\tGHI\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated\r\n\tJKL\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated   \t\r\n\tMNO\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated\t\t\thello\tPQR\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @-

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.format ORDER BY s, x FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --query="DROP TABLE test.format"
