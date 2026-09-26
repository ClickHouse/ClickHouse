#!/usr/bin/env bash
# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_db="${CLICKHOUSE_DATABASE}_db_user"
user_table="${CLICKHOUSE_DATABASE}_table_user"
db_hidden="${CLICKHOUSE_DATABASE}_2"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_db};"
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_table};"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${db_hidden};"
}

trap cleanup EXIT

cleanup

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.visible_table_1 (id UInt32) ENGINE = MergeTree() ORDER BY id;"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.visible_table_2 (id UInt32) ENGINE = MergeTree() ORDER BY id;"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${db_hidden};"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${db_hidden}.hidden_table (id UInt32) ENGINE = MergeTree() ORDER BY id;"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.visible_table_1 VALUES (1);"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.visible_table_2 VALUES (2);"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${db_hidden}.hidden_table VALUES (3);"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_db};"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user_db} IDENTIFIED WITH no_password;"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.parts TO ${user_db};"
$CLICKHOUSE_CLIENT --query "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.* TO ${user_db};"

echo "=== Per-db SHOW TABLES grant user sees all tables in the granted database ==="
$CLICKHOUSE_CLIENT --user="${user_db}" \
    --query "SELECT DISTINCT table FROM system.parts WHERE active AND database = '${CLICKHOUSE_DATABASE}' ORDER BY table;"

echo "=== Per-db SHOW TABLES grant user does NOT see tables in other databases ==="
$CLICKHOUSE_CLIENT --user="${user_db}" \
    --query "SELECT count() FROM system.parts WHERE active AND database = '${db_hidden}';"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_table};"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user_table} IDENTIFIED WITH no_password;"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.parts TO ${user_table};"
$CLICKHOUSE_CLIENT --query "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.visible_table_1 TO ${user_table};"

echo "=== Per-table SHOW TABLES grant user sees only the granted table ==="
$CLICKHOUSE_CLIENT --user="${user_table}" \
    --query "SELECT DISTINCT table FROM system.parts WHERE active AND database = '${CLICKHOUSE_DATABASE}' ORDER BY table;"
