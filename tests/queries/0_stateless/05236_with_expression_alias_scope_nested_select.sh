#!/usr/bin/env bash
# An expression alias declared by a `WITH` clause - `WITH [7] AS nested` rather than
# `WITH nested AS (SELECT ...)` - stays visible in the nested `SELECT`s of the query, as the
# analyzer resolves it there. `AddDefaultDatabaseVisitor` used to forget every alias of a `SELECT`
# when it descended into a nested one, so on the paths that persist or replay a query - a mutation
# command, and the definition a `SQL UDF` expansion leaves behind - such a name was qualified to a
# table of the database owning the definition, and the answer differed from the live query's.
# The alias is inherited by the same rule as a common table expression's name: a nested `SELECT`
# with `enable_global_with_statement = 0` sees the table again.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `SQL UDF` is server-global, so the name carries the database to stay parallel-safe.
F_ALIAS="f_with_expression_alias_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
-- A table of the same name, so that the alias losing means reading it rather than an error.
CREATE TABLE nested (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO nested VALUES (5);

CREATE TABLE t (id UInt8, v UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 0);

-- 100 = the alias won, 10 = the table won.
SELECT 'live', (WITH [7] AS nested SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10));

ALTER TABLE t UPDATE v = (WITH [7] AS nested SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'mutation', v FROM t;

-- The nested \`SELECT\` stops inheriting, so the name is a table name again - in the live query and
-- in the stored command alike.
SELECT 'live off', (WITH [7] AS nested SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 SETTINGS enable_global_with_statement = 0));

ALTER TABLE t UPDATE v = (WITH [7] AS nested SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 SETTINGS enable_global_with_statement = 0)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'mutation off', v FROM t;

-- The narrow pass that repairs a definition after \`SQL UDF\` expansion follows the same rule: the
-- alias is not qualified, and the view answers the same as the live query.
CREATE FUNCTION ${F_ALIAS} AS () -> (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10);
CREATE VIEW v_udf AS WITH [7] AS nested SELECT ${F_ALIAS}() AS r;
SELECT 'view', r FROM v_udf;
SELECT 'stored alias is bare', position(create_table_query, 'nested') > 0 AND position(create_table_query, '.nested') = 0 AND position(create_table_query, '.\`nested\`') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_udf';

-- A bare name that is not an alias is still qualified.
CREATE VIEW v_table AS SELECT (SELECT toUInt8(5 IN nested)) AS r;
SELECT 'stored table is qualified', position(create_table_query, '.nested') > 0 OR position(create_table_query, '.\`nested\`') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_table';

DROP FUNCTION ${F_ALIAS};
EOF
