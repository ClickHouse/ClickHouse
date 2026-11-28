#!/usr/bin/env bash
# https://github.com/ClickHouse/ClickHouse/issues/82084
#
# A two-part or longer identifier whose first part names a table is not owned by that table: when the
# table has no such column, the other readings are tried, including a column literally named with a
# dot in another table expression.
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

# `db.x` resolves to the sibling column named `db.x`.
$CLICKHOUSE_CLIENT --query "
SELECT ${DB}.x FROM ${DB}.${DB}, (SELECT 1 AS \`${DB}.x\`)
SETTINGS joined_subquery_requires_alias = 0;
"

# `db.x.y` resolves to the sibling column named `db.x.y`.
$CLICKHOUSE_CLIENT --query "
SELECT ${DB}.x.y FROM ${DB}.${DB}, (SELECT 1 AS \`${DB}.x.y\`)
SETTINGS joined_subquery_requires_alias = 0;
"

# `db.db` resolves to the sibling column named `db.db`.
$CLICKHOUSE_CLIENT --query "
SELECT ${DB}.${DB} FROM ${DB}.${DB}, (SELECT 1 AS \`${DB}.${DB}\`)
SETTINGS joined_subquery_requires_alias = 0;
"

# The qualifier of \`db.db.*\` is not found as an expression and is bound as a table expression next.
$CLICKHOUSE_CLIENT --query "
SELECT ${DB}.${DB}.* FROM ${DB}.${DB};
"

# The dotted column is still reachable under its own qualification.
$CLICKHOUSE_CLIENT --query "
SELECT t.\`${DB}.x\` FROM ${DB}.${DB}, (SELECT 1 AS \`${DB}.x\`) AS t;
"

# `db.db.*` expands the sibling tuple column, the plain `db.db` is the tuple itself.
$CLICKHOUSE_CLIENT --query "
SELECT ${DB}.${DB}.*, ${DB}.${DB} FROM ${DB}.${DB}, (SELECT tuple(1) AS \`${DB}.${DB}\`) AS t;
"
