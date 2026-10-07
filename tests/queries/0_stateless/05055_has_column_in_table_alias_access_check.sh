#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `Alias` table reports its target's columns, so `hasColumnInTable` on the alias must require
# `SHOW COLUMNS` on the target, exactly as `DESCRIBE` of the same alias does.
# User and table names are scoped to ${CLICKHOUSE_DATABASE} so the test is parallel-safe.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.one TO ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.secret (id UInt64, password String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_alias ENGINE = Alias('${CLICKHOUSE_DATABASE}', 'secret')"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.secret_alias TO ${user}"

run_as_user() { ${CLICKHOUSE_CLIENT} --user "${user}" --password password "$@"; }

echo "-- SHOW COLUMNS on the alias only: denied (existing column of the target)"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'secret_alias', 'password')" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
echo "-- SHOW COLUMNS on the alias only: denied (non-existing column, no oracle)"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'secret_alias', 'not_a_column')" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
echo "-- SHOW COLUMNS on the alias only: DESCRIBE is denied too"
run_as_user --query "DESCRIBE ${CLICKHOUSE_DATABASE}.secret_alias" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.secret TO ${user}"

echo "-- SHOW COLUMNS on the target: existing column"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'secret_alias', 'password')"
echo "-- SHOW COLUMNS on the target: non-existing column"
run_as_user --query "SELECT hasColumnInTable('${CLICKHOUSE_DATABASE}', 'secret_alias', 'not_a_column')"
echo "-- SHOW COLUMNS on the target: DESCRIBE works too"
run_as_user --query "DESCRIBE TABLE ${CLICKHOUSE_DATABASE}.secret_alias FORMAT TSV" | wc -l

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.secret_alias"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.secret"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
