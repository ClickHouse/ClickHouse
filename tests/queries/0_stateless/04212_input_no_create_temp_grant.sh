#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
table="t_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (a UInt32, b String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.${table} TO ${user}"

# `input` must work without the `CREATE TEMPORARY TABLE` grant: it does not create a temporary table,
# it only reads the data stream attached to the INSERT query.
${CLICKHOUSE_CLIENT} --query "SELECT number::UInt32 AS a, 'val' AS b FROM numbers(3) FORMAT Native" | \
    ${CLICKHOUSE_CURL} -sS \
        "${CLICKHOUSE_URL}&user=${user}&password=hello&query=INSERT+INTO+${CLICKHOUSE_DATABASE}.${table}+SELECT+*+FROM+input(%27a+UInt32%2C+b+String%27)+FORMAT+Native" \
        --data-binary @-

${CLICKHOUSE_CLIENT} --user "${user}" --password hello --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.${table} ORDER BY a"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
