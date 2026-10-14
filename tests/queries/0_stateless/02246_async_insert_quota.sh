#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

role="r_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
quota="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts_02246"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${role}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts_02246(a UInt32, s String) ENGINE = Memory"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${role}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON async_inserts_02246 TO ${role}"
${CLICKHOUSE_CLIENT} -q "GRANT ${role} to ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} FOR INTERVAL 100 YEAR MAX QUERY INSERTS = 2 TO ${role}"

${CLICKHOUSE_CLIENT} --user "${user}" --async_insert 1 -q "INSERT INTO async_inserts_02246 VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --user "${user}" --async_insert 1 -q "INSERT INTO async_inserts_02246 VALUES (2, 'b')"
${CLICKHOUSE_CLIENT} --user "${user}" --async_insert 1 -q "INSERT INTO async_inserts_02246 VALUES (3, 'c')" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

sleep 1.0

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM async_inserts_02246"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts_02246"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${role}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
