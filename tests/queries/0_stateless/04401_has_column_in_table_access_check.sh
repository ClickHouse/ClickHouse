#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# hasColumnInTable must require SHOW COLUMNS, like DESCRIBE / SHOW CREATE TABLE.
# User and table names are scoped to ${CLICKHOUSE_DATABASE} so the test is parallel-safe.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.one TO ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.secret (id UInt64, password String) ENGINE = MergeTree ORDER BY id"

run_as_user() { ${CLICKHOUSE_CLIENT} --user "${user}" --password password "$@"; }

echo "-- without SHOW COLUMNS: denied (existing column)"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'secret', 'password')" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
echo "-- without SHOW COLUMNS: denied for a non-existing table (no existence oracle)"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'no_such_table', 'id')" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
echo "-- without SHOW COLUMNS: denied for a non-existing database (no existence oracle)"
run_as_user --query "SELECT hasColumnInTable('no_such_db_${CLICKHOUSE_DATABASE}', 'secret', 'id')" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.secret TO ${user}"

echo "-- with SHOW COLUMNS: existing column"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'secret', 'password')"
echo "-- with SHOW COLUMNS: non-existing column"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'secret', 'not_a_column')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.secret"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
