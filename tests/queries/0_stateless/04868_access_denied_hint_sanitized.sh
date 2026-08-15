#!/usr/bin/env bash

# The access-denied hint may name only objects the caller is allowed to see: it keeps the database
# and table taken from the query itself, but lists no column the caller cannot SHOW.
# `filterAccessElementForHints` is all-or-nothing, so a partial SHOW COLUMNS grant hides every
# column, including the granted one.
# Migrated from tests/integration/test_access_denied_hint_sanitized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
hint_db="${db}_hint"
no_show_db="${db}_no_show"
user_cols="user_cols_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_table="user_table_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_db="user_db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_system="user_system_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Print only the hint the server sent, so the expected text states exactly what the message may
# contain and any leaked name fails the diff. The `Received from` line is the client's rendering of
# the server exception; the server also echoes the same text as a log event (--send_logs_level is
# `warning` by default) on a line that additionally carries the version, the source port and the
# failing query, so the column names would reappear there.
hint()
{
    grep -F 'Received from' | grep -m1 -oP 'Not enough privileges\..*'
}

names()
{
    sed "s/${no_show_db}/<no_show_db>/g; s/${hint_db}/<hint_db>/g; s/${db}/<db>/g; \
         s/${user_cols}/<user_cols>/g; s/${user_table}/<user_table>/g; \
         s/${user_db}/<user_db>/g; s/${user_system}/<user_system>/g"
}

echo "--- a partial SHOW COLUMNS grant hides every column, including the granted one ---"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${db}.t_cols (col_alpha UInt32, col_beta UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE USER OR REPLACE ${user_cols}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON ${db}.t_cols TO ${user_cols}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS(col_alpha) ON ${db}.t_cols TO ${user_cols}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT(col_alpha) ON ${db}.t_cols TO ${user_cols}"
${CLICKHOUSE_CLIENT} --user "${user_cols}" -q "SELECT col_alpha, col_beta FROM ${db}.t_cols" 2>&1 | hint | names

echo "--- granting SHOW COLUMNS on the whole table makes the columns showable ---"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON ${db}.t_cols TO ${user_cols}"
${CLICKHOUSE_CLIENT} --user "${user_cols}" -q "SELECT col_alpha, col_beta FROM ${db}.t_cols" 2>&1 | hint | names

echo "--- the database and table of the query are kept ---"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${hint_db}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${hint_db}.t_hidden (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE USER OR REPLACE ${user_table}"
${CLICKHOUSE_CLIENT} --user "${user_table}" -q "SELECT * FROM ${hint_db}.t_hidden" 2>&1 | hint | names

echo "--- the database name of a CREATE DATABASE is kept ---"
${CLICKHOUSE_CLIENT} -q "CREATE USER OR REPLACE ${user_db}"
${CLICKHOUSE_CLIENT} --user "${user_db}" -q "CREATE DATABASE ${no_show_db}" 2>&1 | hint | names

echo "--- a system table names no column either ---"
${CLICKHOUSE_CLIENT} -q "CREATE USER OR REPLACE ${user_system}"
${CLICKHOUSE_CLIENT} --user "${user_system}" -q "SELECT * FROM system.clusters" 2>&1 | hint | names

${CLICKHOUSE_CLIENT} -q "DROP USER ${user_cols}, ${user_table}, ${user_db}, ${user_system}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${hint_db}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${db}.t_cols"
