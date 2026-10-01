#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104864
# Row policy parsers must reject prefix wildcards (`db*.*`, `table*`) and a bare `.`,
# because RowPolicyName cannot represent them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POLICY="${CLICKHOUSE_DATABASE}_policy"

function query()
{
    $CLICKHOUSE_CLIENT --query "$1"
}

function show_create()
{
    query "SHOW CREATE POLICY $1" | sed "s/${CLICKHOUSE_DATABASE}/db/g"
}

function expect_syntax_error()
{
    query "$1" 2>&1 | grep -o -m1 'SYNTAX_ERROR'
}

echo "-- valid: db.table"
query "CREATE ROW POLICY ${POLICY} ON ${CLICKHOUSE_DATABASE}.mytable USING 1 TO default"
show_create "${POLICY} ON ${CLICKHOUSE_DATABASE}.mytable"
query "DROP ROW POLICY ${POLICY} ON ${CLICKHOUSE_DATABASE}.mytable"

echo "-- valid: db.*"
query "CREATE ROW POLICY ${POLICY} ON ${CLICKHOUSE_DATABASE}.* USING 1 TO default"
show_create "${POLICY} ON ${CLICKHOUSE_DATABASE}.*"
echo "-- valid: SHOW POLICIES ON *.* and ON db.*"
query "SHOW POLICIES ON *.*" | grep -c -F "${POLICY}"
query "SHOW POLICIES ON ${CLICKHOUSE_DATABASE}.*" | grep -c -F "${POLICY}"
query "DROP ROW POLICY ${POLICY} ON ${CLICKHOUSE_DATABASE}.*"

echo "-- valid: table in the current database"
query "CREATE ROW POLICY ${POLICY} ON mytable USING 1 TO default"
show_create "${POLICY} ON mytable"
query "DROP ROW POLICY ${POLICY} ON mytable"

echo "-- invalid: prefix wildcards"
expect_syntax_error "CREATE ROW POLICY ${POLICY} ON mydb*.* USING 1 TO default"
expect_syntax_error "CREATE ROW POLICY ${POLICY} ON mydb*.mytable USING 1 TO default"
expect_syntax_error "CREATE ROW POLICY ${POLICY} ON mydb*.mytable* USING 1 TO default"
expect_syntax_error "CREATE ROW POLICY ${POLICY} ON mydb.mytable* USING 1 TO default"
expect_syntax_error "CREATE ROW POLICY ${POLICY} ON mytable* USING 1 TO default"
expect_syntax_error "ALTER ROW POLICY IF EXISTS ${POLICY} ON mydb*.* TO default"
expect_syntax_error "ALTER ROW POLICY IF EXISTS ${POLICY} ON mytable* TO default"
expect_syntax_error "DROP ROW POLICY IF EXISTS ${POLICY} ON mydb*.*"
expect_syntax_error "DROP ROW POLICY IF EXISTS ${POLICY} ON mytable*"
expect_syntax_error "SHOW POLICIES ON mydb*.*"
expect_syntax_error "SHOW POLICIES ON mytable*"
expect_syntax_error "SHOW CREATE POLICY ${POLICY} ON mydb*.*"
expect_syntax_error "SHOW CREATE POLICY ${POLICY} ON mytable*"

echo "-- invalid: bare dot must not shrink to the current database"
expect_syntax_error "CREATE ROW POLICY ${POLICY} ON . USING 1 TO default"
expect_syntax_error "ALTER ROW POLICY IF EXISTS ${POLICY} ON . TO default"
expect_syntax_error "DROP ROW POLICY IF EXISTS ${POLICY} ON ."
