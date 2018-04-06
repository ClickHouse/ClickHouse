#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query="
    DROP TABLE IF EXISTS test.numbers_100k;
    CREATE VIEW test.numbers_100k AS SELECT * FROM system.numbers LIMIT 100000;
";

STEP_MULTIPLIER=25
if [ -n "$DBMS_TESTS_UNDER_VALGRIND" ]; then
    STEP_MULTIPLIER=1000
fi

for i in $(seq 1000000 $((20000 * $STEP_MULTIPLIER)) 10000000 && seq 10100000 $((100000 * $STEP_MULTIPLIER)) 50000000); do
    $CLICKHOUSE_CLIENT --max_memory_usage=$i --query="
        SELECT intDiv(number, 5) AS k, max(toString(number)) FROM remote('127.0.0.{2,3}', test.numbers_100k) GROUP BY k ORDER BY k LIMIT 1;
    " 2> /dev/null;
    CODE=$?;
    [ "$CODE" -ne "241" ] && [ "$CODE" -ne "0" ] && echo "Fail" && break;
done | uniq

$CLICKHOUSE_CLIENT --query="DROP TABLE test.numbers_100k;";
