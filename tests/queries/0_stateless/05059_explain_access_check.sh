#!/usr/bin/env bash
# `EXPLAIN QUERY TREE` and `EXPLAIN SYNTAX` must not expose column names and types of tables
# the user has no right to see (https://github.com/ClickHouse/ClickHouse/issues/78938).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS explain_access_view;
    DROP TABLE IF EXISTS explain_access_table;

    CREATE TABLE explain_access_table (secret_column_1 Int32, secret_column_2 String) ENGINE = MergeTree ORDER BY secret_column_1;
    CREATE VIEW explain_access_view AS SELECT secret_column_1 FROM explain_access_table;

    CREATE USER ${username} NOT IDENTIFIED;
"

as_user()
{
    ${CLICKHOUSE_CLIENT} --user="${username}" --enable_analyzer=1 --query "$1" 2>&1
}

# The error must be ACCESS_DENIED, and must not disclose the column names. The probe query
# itself may mention a column and is echoed back both by the client and in the forwarded server
# log line, so check only the exception text cut before the first parenthesized annotation.
expect_denied()
{
    local output
    output=$(as_user "$1")
    if [[ "${output}" != *"ACCESS_DENIED"* ]]; then
        echo "FAIL: expected ACCESS_DENIED: ${output}"
        return
    fi
    local messages
    messages=$(grep -oE "DB::Exception: [^(]*" <<< "${output}")
    if [[ "${messages}" == *"secret_column"* ]]; then
        echo "FAIL: the error message leaks column names: ${output}"
    else
        echo "denied"
    fi
}

expect_ok()
{
    local output
    output=$(as_user "$1")
    if [[ "${output}" == *"Exception"* ]]; then
        echo "FAIL: expected success: ${output}"
    else
        echo "ok"
    fi
}

echo "-- no grants"
expect_denied "EXPLAIN QUERY TREE SELECT * FROM explain_access_table"
expect_denied "EXPLAIN QUERY TREE SELECT count() FROM explain_access_table"
expect_denied "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM explain_access_table"
expect_denied "EXPLAIN QUERY TREE SELECT 1 WHERE 1 IN (SELECT secret_column_1 FROM explain_access_table)"
expect_denied "EXPLAIN QUERY TREE SELECT * FROM explain_access_view"

echo "-- no grants: whether a column exists must not be observable through the error kind"
expect_denied "SELECT secret_column_1 FROM explain_access_table"
expect_denied "SELECT no_such_column FROM explain_access_table"

echo "-- no grants: unresolved dumps reveal no table metadata and stay allowed"
expect_unresolved_ok()
{
    local output
    output=$(as_user "$1")
    if [[ "${output}" == *"Exception"* ]]; then
        echo "FAIL: expected success: ${output}"
    elif [[ "${output}" == *"secret_column"* ]]; then
        echo "FAIL: the unresolved dump leaks column names: ${output}"
    else
        echo "ok"
    fi
}
expect_unresolved_ok "EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM explain_access_table"
expect_unresolved_ok "EXPLAIN SYNTAX SELECT * FROM explain_access_table"

echo "-- a grant on a single column makes the table structure visible"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(secret_column_1) ON explain_access_table TO ${username}"
expect_ok "EXPLAIN QUERY TREE SELECT secret_column_1 FROM explain_access_table"
expect_ok "EXPLAIN QUERY TREE SELECT * FROM explain_access_table"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(secret_column_1) ON explain_access_table FROM ${username}"

echo "-- SHOW COLUMNS also makes the structure visible, but reading is still denied"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON explain_access_table TO ${username}"
expect_ok "EXPLAIN QUERY TREE SELECT * FROM explain_access_table"
as_user "SELECT * FROM explain_access_table" | grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON explain_access_table FROM ${username}"

echo "-- a grant on the view is enough for the view, the table stays protected"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON explain_access_view TO ${username}"
expect_ok "EXPLAIN QUERY TREE SELECT * FROM explain_access_view"
expect_denied "EXPLAIN QUERY TREE SELECT * FROM explain_access_table"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS explain_access_view;
    DROP TABLE IF EXISTS explain_access_table;
"
