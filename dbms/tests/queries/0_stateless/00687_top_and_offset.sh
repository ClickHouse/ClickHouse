#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test(val Int64) engine = Memory;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (1);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (2);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (3);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (4);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (5);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (6);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (7);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (8);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test VALUES (9);"

${CLICKHOUSE_CLIENT} --query "SELECT TOP 2 * FROM test ORDER BY val;"
${CLICKHOUSE_CLIENT} --query "SELECT TOP (2) * FROM test ORDER BY val;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY val LIMIT 2 OFFSET 2;"

echo `${CLICKHOUSE_CLIENT} --query "SELECT TOP 2 * FROM test ORDER BY val LIMIT 2;" 2>&1 | grep -c "Code: 406"`
echo `${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY val LIMIT 2,3 OFFSET 2;" 2>&1 | grep -c "Code: 62"`

${CLICKHOUSE_CLIENT} --query "DROP TABLE test;"
