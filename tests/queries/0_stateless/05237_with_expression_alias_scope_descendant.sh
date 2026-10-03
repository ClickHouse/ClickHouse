#!/usr/bin/env bash
# An expression alias of a `WITH` clause is inherited by the nested `SELECT`s at any depth of the
# expression - `WITH tuple([7] AS nested) AS wrapper`, not only `WITH [7] AS nested` - because the
# analyzer collects it through the whole expression. It stops at a lambda and at a nested query,
# so an alias declared inside one of those is a table name in the nested `SELECT`s, and the
# mutation and persisted-definition paths have to qualify it there and only there.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `SQL UDF` is server-global, so the name carries the database to stay parallel-safe.
F_ALIAS="f_with_expression_alias_descendant_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
-- Tables of the same names, so that the alias losing means reading one rather than an error.
CREATE TABLE nested (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE lam (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE sub (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO nested VALUES (5);
INSERT INTO lam VALUES (5);
INSERT INTO sub VALUES (5);

CREATE TABLE t (id UInt8, v UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0);

-- 100 = the alias won, 10 = the table won.
SELECT 'live', (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10));
ALTER TABLE t UPDATE v = (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10)) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'mutation', v FROM t WHERE id = 1;

SELECT 'live array', (WITH [7 AS nested] AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10));
ALTER TABLE t UPDATE v = (WITH [7 AS nested] AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10)) WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT 'mutation array', v FROM t WHERE id = 2;

-- A definition written out by hand goes through the main pass, which has to agree.
CREATE VIEW v_direct AS WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10) AS r;
SELECT 'view direct', r FROM v_direct;

-- The narrow pass that repairs a definition after \`SQL UDF\` expansion follows the same rule.
CREATE FUNCTION ${F_ALIAS} AS () -> (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10);
CREATE VIEW v_udf AS WITH tuple([7] AS nested) AS wrapper SELECT ${F_ALIAS}() AS r;
SELECT 'view', r FROM v_udf;
SELECT 'stored alias is bare', position(create_table_query, 'nested') > 0 AND position(create_table_query, '.nested') = 0 AND position(create_table_query, '.\`nested\`') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_udf';

-- The nested \`SELECT\` stops inheriting, so the name is a table name again.
SELECT 'live off', (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 SETTINGS enable_global_with_statement = 0));
ALTER TABLE t UPDATE v = (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 SETTINGS enable_global_with_statement = 0)) WHERE id = 3 SETTINGS mutations_sync = 2;
SELECT 'mutation off', v FROM t WHERE id = 3;

-- An alias the analyzer does not collect - declared inside a lambda, and inside a nested query -
-- is a table name in the nested \`SELECT\`, in the live query and in the stored command alike.
SELECT 'live lambda', (WITH (y -> y + ([7] AS lam)[1]) AS f SELECT (SELECT toUInt8(7 IN lam) * 100 + toUInt8(5 IN lam) * 10));
ALTER TABLE t UPDATE v = (WITH (y -> y + ([7] AS lam)[1]) AS f SELECT (SELECT toUInt8(7 IN lam) * 100 + toUInt8(5 IN lam) * 10)) WHERE id = 4 SETTINGS mutations_sync = 2;
SELECT 'mutation lambda', v FROM t WHERE id = 4;

SELECT 'live subquery', (WITH (SELECT ([7] AS sub)[1]) AS s SELECT (SELECT toUInt8(7 IN sub) * 100 + toUInt8(5 IN sub) * 10));
ALTER TABLE t UPDATE v = (WITH (SELECT ([7] AS sub)[1]) AS s SELECT (SELECT toUInt8(7 IN sub) * 100 + toUInt8(5 IN sub) * 10)) WHERE id = 5 SETTINGS mutations_sync = 2;
SELECT 'mutation subquery', v FROM t WHERE id = 5;

DROP FUNCTION ${F_ALIAS};
EOF
