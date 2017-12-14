#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT 1"
$CLICKHOUSE_CLIENT --query="SELECT 1;"
$CLICKHOUSE_CLIENT --query="SELECT 1; "
$CLICKHOUSE_CLIENT --query="SELECT 1 ; "

$CLICKHOUSE_CLIENT --query="SELECT 1; S" 2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT 1; SELECT 2" 2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT 1; SELECT 2;" 2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT 1; SELECT 2; SELECT" 2>&1 | grep -o 'Syntax error'

$CLICKHOUSE_CLIENT -n --query="SELECT 1; S" 2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT -n --query="SELECT 1; SELECT 2"
$CLICKHOUSE_CLIENT -n --query="SELECT 1; SELECT 2;"
$CLICKHOUSE_CLIENT -n --query="SELECT 1; SELECT 2; SELECT" 2>&1 | grep -o 'Syntax error'

$CLICKHOUSE_CLIENT -n --query="DROP TABLE IF EXISTS test.t; CREATE TABLE test.t (x UInt64) ENGINE = TinyLog;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test.t VALUES (1),(2),(3);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.t"
$CLICKHOUSE_CLIENT --query="INSERT INTO test.t VALUES" <<< "(4),(5),(6)"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.t"

$CLICKHOUSE_CLIENT -n --query="INSERT INTO test.t VALUES (1),(2),(3);"
$CLICKHOUSE_CLIENT -n --query="SELECT * FROM test.t"
$CLICKHOUSE_CLIENT -n --query="INSERT INTO test.t VALUES" <<< "(4),(5),(6)"
$CLICKHOUSE_CLIENT -n --query="SELECT * FROM test.t"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1;"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1; "
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1 ; "

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1; S" 2>&1 | grep -o 'Syntax error'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1; SELECT 2" 2>&1 | grep -o 'Syntax error'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1; SELECT 2;" 2>&1 | grep -o 'Syntax error'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 1; SELECT 2; SELECT" 2>&1 | grep -o 'Syntax error'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO test.t VALUES (1),(2),(3);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.t"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT" -d "INTO test.t VALUES (4),(5),(6);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.t"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+test.t+VALUES" -d "(7),(8),(9)"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.t"

$CLICKHOUSE_CLIENT -n --query="DROP TABLE test.t;"
