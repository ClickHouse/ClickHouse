#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse-client --query="DROP TABLE IF EXISTS test.format"
clickhouse-client --query="CREATE TABLE test.format (s String, x FixedString(3)) ENGINE = Memory"

echo -ne '\tABC\n' | curl -sS "http://localhost:8123/?query=INSERT+INTO+test.format+FORMAT+TabSeparated" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated\n\tDEF\n' | curl -sS "http://localhost:8123/" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated hello\tGHI\n' | curl -sS "http://localhost:8123/" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated\r\n\tJKL\n' | curl -sS "http://localhost:8123/" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated   \t\r\n\tMNO\n' | curl -sS "http://localhost:8123/" --data-binary @-
echo -ne 'INSERT INTO test.format FORMAT TabSeparated\t\t\thello\tPQR\n' | curl -sS "http://localhost:8123/" --data-binary @-

clickhouse-client --query="SELECT * FROM test.format ORDER BY s, x FORMAT JSONEachRow"
clickhouse-client --query="DROP TABLE test.format"
