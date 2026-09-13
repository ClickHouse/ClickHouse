#!/usr/bin/env bash
# Regression test: `system.tables.dependencies_database` / `dependencies_table` must not expose a
# dependent view the user is not allowed to list. The source table and the view live in different
# databases, and the user is granted `SHOW TABLES` on the database of the source only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

DB_VIEWS="${CLICKHOUSE_DATABASE}_views"
USER="${CLICKHOUSE_DATABASE}_limited"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_VIEWS}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_VIEWS}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.src (id UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE VIEW ${DB_VIEWS}.v AS SELECT * FROM ${CLICKHOUSE_DATABASE}.src"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"

# The random database name of the view is replaced by a stable label, so the reference file is deterministic.
dependents_of_src()
{
    $CLICKHOUSE_CLIENT "$@" -q "
        SELECT arraySort(arrayMap((d, t) -> concat(if(d = '${DB_VIEWS}', 'views_db', d), '.', t), dependencies_database, dependencies_table))
        FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'src'"
}

echo -n 'default user: '
dependents_of_src

echo -n 'user without SHOW TABLES on the view: '
dependents_of_src --user "${USER}"

$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${DB_VIEWS}.v TO ${USER}"

echo -n 'the same user after the grant: '
dependents_of_src --user "${USER}"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_VIEWS}"
