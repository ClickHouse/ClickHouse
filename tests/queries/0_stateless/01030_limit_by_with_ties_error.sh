#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=none/g')

$CLICKHOUSE_CLIENT --query="SELECT * FROM (SELECT number % 5 AS a, count() AS b, c FROM numbers(10) ARRAY JOIN [1,2] AS c GROUP BY a,c) AS table ORDER BY a LIMIT 3 WITH TIES BY a" 2>&1 | grep -q "Code: 498." && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT --query="SELECT * FROM VALUES('Phrase String, Payload String', ('hello', 'x'), ('world', 'x'), ('hello', 'z'), ('upyachka', 'a'), ('test', 'b'), ('foo', 'c'), ('bar', 'd')) ORDER BY Payload LIMIT 1 WITH TIES BY Phrase LIMIT 5;" 2>&1 | grep -q "Code: 498." && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT --query="SELECT * FROM VALUES('Phrase String, Payload String', ('hello', 'x'), ('world', 'x'), ('hello', 'z'), ('upyachka', 'a'), ('test', 'b'), ('foo', 'c'), ('bar', 'd')) ORDER BY Payload LIMIT 1 BY Phrase LIMIT 5 WITH TIES"

$CLICKHOUSE_CLIENT --query="SELECT TOP 5 WITH TIES * FROM VALUES('Phrase String, Payload String', ('hello', 'x'), ('world', 'x'), ('hello', 'z'), ('upyachka', 'a'), ('test', 'b'), ('foo', 'c'), ('bar', 'd')) ORDER BY Payload LIMIT 1 BY Phrase"
