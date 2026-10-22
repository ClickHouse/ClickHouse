#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104864
# `GRANT ... ON db*.table` used to be silently rewritten to `db.table*`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="user_${CLICKHOUSE_DATABASE}"

function show_grants()
{
    $CLICKHOUSE_CLIENT --query "SHOW GRANTS FOR ${USER_NAME}" | sed "s/${USER_NAME}/user/"
}

function expect_syntax_error()
{
    $CLICKHOUSE_CLIENT --query "$1" 2>&1 | grep -o -m1 'SYNTAX_ERROR'
}

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER_NAME}"

echo "-- valid: db.*"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.* TO ${USER_NAME}"
show_grants

echo "-- valid: mydb*.*"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON mydb*.* TO ${USER_NAME}"
show_grants

echo "-- valid: db.mytable*"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM ${USER_NAME}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.mytable* TO ${USER_NAME}"
show_grants

echo "-- valid: *.*"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM ${USER_NAME}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON *.* TO ${USER_NAME}"
show_grants

echo "-- invalid: database wildcard with a table name"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM ${USER_NAME}"
expect_syntax_error "GRANT SELECT ON db*.table TO ${USER_NAME}"
expect_syntax_error "GRANT SELECT ON db*.mytable TO ${USER_NAME}"
expect_syntax_error "GRANT SELECT ON mydb*.mytable* TO ${USER_NAME}"

echo "-- nothing was granted by the rejected queries"
show_grants

$CLICKHOUSE_CLIENT --query "DROP USER ${USER_NAME}"
