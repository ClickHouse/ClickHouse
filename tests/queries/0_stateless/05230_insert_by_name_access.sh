#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

set -e

table="insert_by_name_access_05230"
user="insert_by_name_user_05230"
password="password"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
}
trap cleanup EXIT

cleanup
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (a UInt64, b String DEFAULT 'default') ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '${password}'"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT(a) ON ${table} TO ${user}"

client_as_user="${CLICKHOUSE_CLIENT} --user ${user} --password ${password}"
${client_as_user} --query "INSERT INTO ${table} BY NAME SELECT 42 AS a"
echo "allowed"

if ${client_as_user} --query "INSERT INTO ${table} BY NAME SELECT 'value' AS b" 2>&1 | grep -q "ACCESS_DENIED"; then
    echo "denied"
else
    echo "unexpected_success"
    exit 1
fi
