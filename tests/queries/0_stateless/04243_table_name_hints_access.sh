#!/usr/bin/env bash

# Test for table name hints access control: the cross-database hint search in
# `getExtendedHintForTable` must not leak the existence of tables or databases
# the user does not have `SHOW_TABLES` / `SHOW_DATABASES` on.
# https://github.com/ClickHouse/ClickHouse/issues/93101

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="${CLICKHOUSE_DATABASE}"
secret_db="${CLICKHOUSE_DATABASE}_secret"
# Use unique table names tied to the per-test random database so that
# concurrent runs do not interfere with the hint search (see 03924).
my_table="my_table_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${secret_db}"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${secret_db}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${secret_db}.${my_table} (x UInt64) ENGINE = MergeTree ORDER BY x"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW ON ${db}.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT DROP TABLE ON ${db}.* TO ${user}"

# As the restricted user, attempt to drop a non-existent table whose name matches a table in the secret database.
# The hint must not reveal that ${secret_db}.${my_table} exists.
${CLICKHOUSE_CLIENT} --user "${user}" --password hello -q "DROP TABLE ${db}.${my_table}" 2>&1 \
  | grep 'Received from' \
  | grep -oE '(Maybe you meant|UNKNOWN_TABLE)' \
  | head -1

# Sanity check: the privileged user still sees the cross-database hint.
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${db}.${my_table}" 2>&1 \
  | grep 'Received from' \
  | grep -oP 'Maybe you meant \S+' \
  | sed "s/${secret_db}/<secret_db>/g" \
  | sed "s/${my_table}/<my_table>/g"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${secret_db}.${my_table}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${secret_db}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
