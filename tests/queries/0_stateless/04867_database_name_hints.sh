#!/usr/bin/env bash

# Hints for a mistyped database name (`DatabaseNameHints`) and for an unknown database
# engine, plus the access filter that must not disclose a database the caller cannot see.
# Migrated from tests/integration/test_wrong_db_or_table_name.
# The table-name hints of that module are already covered by
# 03924_table_name_hints_cross_database and 04338_analyzer_unknown_table_cross_database_hint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
engine_db="${db}_engine"
visible_db="${db}_visible"
hidden_db="${db}_hidden"
# Each typo replaces the last character, so it is one edit from its target and shares no
# substring relation with it, which keeps the name substitution below unambiguous. Every
# database of a concurrently running test shares only the `test_` prefix and is therefore many
# more edits away than `NamePrompter`'s (len+2)/3 bound, so the hint stays deterministic (03924).
db_typo="${db%?}q"
hidden_db_typo="${hidden_db%?}q"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Print only the hint the server sent, so the expected text states exactly what the message may
# contain and any leaked name fails the diff. The `Received from` line is the client's rendering
# of the server exception; the server ALSO echoes the same text as a log event
# (--send_logs_level is `warning` by default) on a line carrying the version, the source port and
# the failing query.
hint()
{
    grep -F 'Received from' | grep -m1 -oP "$1.*" \
        | sed "s/${visible_db}/<visible_db>/g; s/${hidden_db}/<hidden_db>/g; \
               s/${hidden_db_typo}/<hidden_db_typo>/g; s/${db_typo}/<db_typo>/g; s/${db}/<db>/g"
}

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${db}.table_test (i Int64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${db}.table_test SELECT 1"

echo "--- SELECT from a mistyped database name ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${db_typo}.table_test LIMIT 1" 2>&1 | hint 'Database'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${db}.table_test"

echo "--- DROP DATABASE with a mistyped name ---"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${db_typo}" 2>&1 | hint 'Database'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${db}.table_test"

echo "--- a valid database engine works ---"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${engine_db} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${engine_db}.table_test (i Int64) ENGINE = MergeTree() ORDER BY i"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${engine_db}.table_test SELECT 1"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${engine_db}.table_test"

echo "--- unknown database engine ---"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${engine_db}_bad ENGINE = Atomic123" 2>&1 | hint 'Unknown database engine'

echo "--- the hint must not disclose a database the user cannot SHOW ---"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${visible_db}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${hidden_db}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${visible_db}.depth (id UInt32, value String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${hidden_db}.depth (id UInt32, secret_data String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE USER OR REPLACE ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${visible_db}.* TO ${user}"
# ${hidden_db_typo} is one edit from ${hidden_db} and six from ${visible_db}, so the restricted
# user must fall back to the visible database rather than name the nearer hidden one.
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT * FROM ${hidden_db_typo}.depth" 2>&1 | hint 'Database'
echo "--- and the privileged user still sees it ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${hidden_db_typo}.depth" 2>&1 | hint 'Database'

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${hidden_db}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${visible_db}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${engine_db}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${db}.table_test"
