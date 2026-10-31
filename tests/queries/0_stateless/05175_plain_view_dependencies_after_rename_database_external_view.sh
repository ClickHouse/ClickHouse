#!/usr/bin/env bash
# Regression test: `RENAME DATABASE` must not move a plain-view dependency edge onto the renamed
# database when the view lives elsewhere. Such a view names its source qualified with the old database
# name, and the rename does not rewrite the stored definition, so the table under the new name has no
# dependents - which is also what a metadata reload or a metadata-only `ALTER` recomputes.
#
# This is a shell test because the name of the renamed database has to be written inside the body of
# the view: a query parameter there would make it a parameterized view instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -ue

DB_BEFORE="${CLICKHOUSE_DATABASE}_before"
DB_AFTER="${CLICKHOUSE_DATABASE}_after"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_BEFORE}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_AFTER}"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_BEFORE} ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB_BEFORE}.src (id UInt64) ENGINE = MergeTree ORDER BY id"

# The view is created in another database, so its source is stored qualified with the database being renamed.
$CLICKHOUSE_CLIENT -q "CREATE VIEW ${CLICKHOUSE_DATABASE}.v AS SELECT * FROM ${DB_BEFORE}.src"

# The random database name of the view is replaced by a stable label, so the reference file is deterministic.
dependents_of_src()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT arraySort(arrayMap((d, t) -> concat(if(d = '${CLICKHOUSE_DATABASE}', 'other_db', d), '.', t), dependencies_database, dependencies_table))
        FROM system.tables WHERE database = '$1' AND name = 'src'"
}

echo -n 'before rename: '
dependents_of_src "${DB_BEFORE}"

$CLICKHOUSE_CLIENT -q "RENAME DATABASE ${DB_BEFORE} TO ${DB_AFTER}"

# Guard the precondition: the rename leaves the stored definition of the view naming the old database.
echo -n 'the view still names the old database: '
$CLICKHOUSE_CLIENT -q "
    SELECT create_table_query LIKE '%${DB_BEFORE}%' AND create_table_query NOT LIKE '%${DB_AFTER}%'
    FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'v'"

echo -n 'after rename: '
dependents_of_src "${DB_AFTER}"

# A metadata-only ALTER recomputes the edge from the stored definition and must agree with the rename.
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.v MODIFY COMMENT 'renamed source database'"

echo -n 'after alter: '
dependents_of_src "${DB_AFTER}"

$CLICKHOUSE_CLIENT -q "DROP VIEW ${CLICKHOUSE_DATABASE}.v"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_AFTER}"
