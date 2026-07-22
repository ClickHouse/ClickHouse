#!/usr/bin/env bash
# https://github.com/ClickHouse/ClickHouse/issues/82084
#
# The qualifier-miss fall-through (a failed lookup behind a table name that also matches a database
# name is allowed to fail, so that the `db.table.column` interpretation is attempted next) must fire
# only when such an interpretation actually exists. A two-part identifier `db.x` against the table
# `db.db` has no `db.table.column` interpretation, so a missing column `x` must still throw
# `UNKNOWN_IDENTIFIER` instead of falling through to a column literally named `db.x` of a sibling
# table expression.
#
# This is a shell test because the sibling column has to be literally named `<db>.x` (one identifier
# with a dot), which cannot be composed from a query parameter (`{db:Identifier}`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE ${DB}.${DB} (id Int32) ENGINE = MergeTree ORDER BY ();
INSERT INTO ${DB}.${DB} VALUES (42);
"

# There is no column `x` in the table `${DB}.${DB}` and no table `${DB}.x`, so the sibling dotted
# column must not be picked up.
$CLICKHOUSE_CLIENT --query "
SELECT ${DB}.x FROM ${DB}.${DB}, (SELECT 1 AS \`${DB}.x\`)
SETTINGS enable_analyzer = 1, joined_subquery_requires_alias = 0;
" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'

# The same with three parts: there is no table \`x\` in the database, so \`db.x.y\` has no
# \`db.table.column\` interpretation either.
$CLICKHOUSE_CLIENT --query "
SELECT ${DB}.x.y FROM ${DB}.${DB}, (SELECT 1 AS \`${DB}.x.y\`)
SETTINGS enable_analyzer = 1, joined_subquery_requires_alias = 0;
" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'

# The dotted column is still reachable under its own qualification.
$CLICKHOUSE_CLIENT --query "
SELECT t.\`${DB}.x\` FROM ${DB}.${DB}, (SELECT 1 AS \`${DB}.x\`) AS t
SETTINGS enable_analyzer = 1;
"
