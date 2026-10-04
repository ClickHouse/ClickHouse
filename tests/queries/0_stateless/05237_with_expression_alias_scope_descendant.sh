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
CREATE TABLE src (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_create ENGINE = MergeTree ORDER BY tuple() AS WITH tuple([7] AS nested) AS wrapper SELECT x, (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10) AS r FROM src;
CREATE MATERIALIZED VIEW mv_modify ENGINE = MergeTree ORDER BY tuple() AS SELECT x, (SELECT toUInt8(7 IN [1]) * 100 + toUInt8(5 IN [1]) * 10) AS r FROM src;
ALTER TABLE mv_modify MODIFY QUERY WITH tuple([7] AS nested) AS wrapper SELECT x, (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10) AS r FROM src;
INSERT INTO src VALUES (1);
SELECT 'materialized view', r FROM mv_create;
SELECT 'modify query', r FROM mv_modify;

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

-- The rest of the same \`WITH\` expression sees an alias declared earlier in it.
SELECT 'live same expression', (WITH tuple([7] AS nested, (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10)) AS wrapper SELECT tupleElement(wrapper, 2));
CREATE VIEW v_same AS WITH tuple([7] AS nested, (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10)) AS wrapper SELECT tupleElement(wrapper, 2) AS r;
SELECT 'view same expression', r FROM v_same;

-- An alias the nested \`SELECT\` declares itself wins: 1 = its own alias won.
SELECT 'live shadowed', (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) WHERE notEmpty([6] AS nested)));
CREATE VIEW v_shadowed AS WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) WHERE notEmpty([6] AS nested)) AS r;
SELECT 'view shadowed', r FROM v_shadowed;
SELECT 'live shadowed top-level', (WITH [7] AS nested SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) WHERE notEmpty([6] AS nested)));
CREATE VIEW v_shadowed_top AS WITH [7] AS nested SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) WHERE notEmpty([6] AS nested)) AS r;
SELECT 'view shadowed top-level', r FROM v_shadowed_top;

-- So does a name that \`ARRAY JOIN\`, \`JOIN ... ON\`, a table function argument or a lambda binds.
SELECT 'live shadowed array join', (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) FROM numbers(1) ARRAY JOIN [6] AS nested));
CREATE VIEW v_shadowed_array_join AS WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) FROM numbers(1) ARRAY JOIN [6] AS nested) AS r;
SELECT 'view shadowed array join', r FROM v_shadowed_array_join;
SELECT 'live shadowed join on', (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) FROM numbers(1) AS a INNER JOIN numbers(1) AS b ON notEmpty([6] AS nested)));
CREATE VIEW v_shadowed_join_on AS WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) FROM numbers(1) AS a INNER JOIN numbers(1) AS b ON notEmpty([6] AS nested)) AS r;
SELECT 'view shadowed join on', r FROM v_shadowed_join_on;
SELECT 'live shadowed table function', (WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) FROM numbers(length([6] AS nested))));
CREATE VIEW v_shadowed_table_function AS WITH tuple([7] AS nested) AS wrapper SELECT (SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) FROM numbers(length([6] AS nested))) AS r;
SELECT 'view shadowed table function', r FROM v_shadowed_table_function;
SELECT 'live lambda parameter', (WITH tuple([7] AS nested) AS wrapper SELECT arrayMap(nested -> toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested), [[6]])[1]);
CREATE VIEW v_lambda_parameter AS WITH tuple([7] AS nested) AS wrapper SELECT arrayMap(nested -> toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested), [[6]])[1] AS r;
SELECT 'view lambda parameter', r FROM v_lambda_parameter;
SELECT 'live lambda alias', (WITH tuple([7] AS nested) AS wrapper SELECT arrayMap(x -> toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) + x * length([6] AS nested), [0])[1]);
CREATE VIEW v_lambda_alias AS WITH tuple([7] AS nested) AS wrapper SELECT arrayMap(x -> toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 + toUInt8(6 IN nested) + x * length([6] AS nested), [0])[1] AS r;
SELECT 'view lambda alias', r FROM v_lambda_alias;
-- A lambda keeps its aliases, also inside a table function argument, so outside it the name is a table of the view's database.
SELECT 'live table function lambda', toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 FROM numbers(arrayMap(x -> x + length([6] AS nested), [0])[1]);
CREATE VIEW v_table_function_lambda AS SELECT toUInt8(7 IN nested) * 100 + toUInt8(5 IN nested) * 10 AS r FROM numbers(arrayMap(x -> x + length([6] AS nested), [0])[1]);
USE system;
SELECT 'view table function lambda', r FROM ${CLICKHOUSE_DATABASE}.v_table_function_lambda;
USE ${CLICKHOUSE_DATABASE};

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
