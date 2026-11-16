#!/usr/bin/env bash
# RENAME COLUMN on tables that an older server stored with an alias in a constraint or a TTL
# (05331_rename_column_alias_capture covers the column defaults).
#
# Such a definition can no longer be written through SQL, so the stored metadata of tables
# created without the aliases is edited into the form the older server wrote.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t_constraint_lambda_alias (a Int64, arr Array(Int64), CONSTRAINT k CHECK arrayAll(x -> ((x + 1) > 0), arr) AND (a > 0)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE db.t_ttl_set_alias (d Date, key UInt64, c0 UInt64, c1 UInt64, value UInt64) ENGINE = MergeTree ORDER BY key TTL d + INTERVAL 1 YEAR GROUP BY key SET value = any(c0 + c1);
CREATE TABLE db.t_subquery_outer_alias (d DateTime, c0 UInt64, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE (c0 > 0) AND (c0 < (SELECT max(x) FROM (SELECT 100 AS x)));
CREATE TABLE db.t_constraint_own_definition (a Int64, CONSTRAINT k CHECK (a + 1) > 0) ENGINE = MergeTree ORDER BY tuple();
"

metadata() { grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include="$1.sql"; }
sed -i 's/x -> ((x + 1) > 0)/x -> ((x + 1 AS b) > 0)/' "$(metadata t_constraint_lambda_alias)"
sed -i 's/any(c0 + c1)/any((c0 AS c1) + c1)/' "$(metadata t_ttl_set_alias)"
sed -i 's/WHERE (c0 > 0)/WHERE ((c0 AS u) > 0)/' "$(metadata t_subquery_outer_alias)"
sed -i 's/CHECK (a + 1) > 0/CHECK (a + 1 AS b) > 0/' "$(metadata t_constraint_own_definition)"
# Without this the renames below would run against definitions without the aliases.
cat "$(metadata t_constraint_lambda_alias)" "$(metadata t_ttl_set_alias)" "$(metadata t_subquery_outer_alias)" "$(metadata t_constraint_own_definition)" \
    | grep -c -F -e 'x + 1 AS b' -e '(c0 AS c1) + c1' -e '(c0 AS u) > 0' -e 'a + 1 AS b'

# Outside column defaults an alias inside a lambda is visible in the whole expression.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "ALTER TABLE db.t_constraint_lambda_alias RENAME COLUMN a TO b" 2>&1 \
    | grep -c -F '`b` is already an alias in constraint `k`'
# A renamed reference in a TTL GROUP BY SET expression that meant the alias with the old name.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "ALTER TABLE db.t_ttl_set_alias RENAME COLUMN c1 TO v" 2>&1 \
    | grep -c -F '`c1` is also an alias in the table TTL'

# An alias does not reach into a subquery, and does not bind inside its own definition.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ALTER TABLE db.t_subquery_outer_alias RENAME COLUMN x TO u;
SELECT position(create_table_query, 'WHERE ((c0 AS u) > 0) AND (c0 < (SELECT max(x) FROM (SELECT 100 AS x)))') > 0 FROM system.tables WHERE database = 'db' AND name = 't_subquery_outer_alias';
ALTER TABLE db.t_constraint_own_definition RENAME COLUMN a TO b;
INSERT INTO db.t_constraint_own_definition (b) VALUES (5);
INSERT INTO db.t_constraint_own_definition (b) VALUES (-5); -- { serverError VIOLATED_CONSTRAINT }
SELECT b FROM db.t_constraint_own_definition;
"

rm -rf "${WORKING_DIR}"
