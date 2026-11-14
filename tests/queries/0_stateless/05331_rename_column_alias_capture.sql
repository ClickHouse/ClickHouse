-- RENAME COLUMN must not make a renamed reference bind to an alias or a lambda parameter.

-- An alias with the new name is visible at a renamed reference.
DROP TABLE IF EXISTS t_default;
CREATE TABLE t_default (c0 Int64, c1 Int64, c2 Int64 DEFAULT (c0 AS u) + c1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_default (c0, c1) VALUES (1, 10);
ALTER TABLE t_default RENAME COLUMN c1 TO u; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_default (c0, c1) VALUES (1, 10);
SELECT 'default', c2 FROM t_default;
DROP TABLE t_default;

DROP TABLE IF EXISTS t_materialized;
CREATE TABLE t_materialized (c0 Int64, c1 Int64, c2 Int64 MATERIALIZED (c0 * 2 AS w) + c1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_materialized (c0, c1) VALUES (2, 8);
ALTER TABLE t_materialized RENAME COLUMN c1 TO w; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_materialized (c0, c1) VALUES (2, 8);
SELECT 'materialized', c2 FROM t_materialized;
DROP TABLE t_materialized;

DROP TABLE IF EXISTS t_alias;
CREATE TABLE t_alias (c0 Int64, c1 Int64, c2 Int64 ALIAS (c0 AS v) + c1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias (c0, c1) VALUES (1, 10);
ALTER TABLE t_alias RENAME COLUMN c1 TO v; -- { serverError BAD_ARGUMENTS }
SELECT 'alias', c2 FROM t_alias;
DROP TABLE t_alias;

DROP TABLE IF EXISTS t_memory;
CREATE TABLE t_memory (c0 Int64, c1 Int64, c2 Int64 DEFAULT (c0 AS u) + c1) ENGINE = Memory;
INSERT INTO t_memory (c0, c1) VALUES (1, 10);
ALTER TABLE t_memory RENAME COLUMN c1 TO u; -- { serverError BAD_ARGUMENTS }
SELECT 'memory', c2 FROM t_memory;
DROP TABLE t_memory;

-- The defaults of all columns share their aliases.
DROP TABLE IF EXISTS t_across;
CREATE TABLE t_across (c0 Int64, c1 Int64, c2 Int64 DEFAULT (c0 AS u) + 1, c3 Int64 DEFAULT c1 * 100) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_across (c0, c1) VALUES (1, 10);
ALTER TABLE t_across RENAME COLUMN c1 TO u; -- { serverError BAD_ARGUMENTS }
SELECT 'across', c2, c3 FROM t_across;
DROP TABLE t_across;

-- A renamed reference that meant an alias with the old name, or the field of an alias.
DROP TABLE IF EXISTS t_old_alias;
CREATE TABLE t_old_alias (c0 Int64, c1 Int64, c2 Int64 DEFAULT (c0 AS c1) + c1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_old_alias (c0, c1) VALUES (1, 10);
ALTER TABLE t_old_alias RENAME COLUMN c1 TO v; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_old_alias (c0, c1) VALUES (1, 10);
SELECT 'old alias', c2 FROM t_old_alias;
DROP TABLE t_old_alias;

DROP TABLE IF EXISTS t_alias_field;
CREATE TABLE t_alias_field (`x.y` Int64, c2 Int64 DEFAULT (CAST(tuple(5), 'Tuple(y Int64)') AS x).y * 0 + x.y) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_alias_field RENAME COLUMN `x.y` TO `x.z`; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_alias_field (`x.y`) VALUES (100);
SELECT 'alias field', c2 FROM t_alias_field;
DROP TABLE t_alias_field;

-- A compound x.y and a quoted `x.y` in one lambda with the parameter x resolve to the same thing.
DROP TABLE IF EXISTS t_field_pair;
CREATE TABLE t_field_pair (`x.y` Int64, arr Array(Tuple(y Int64)), c2 Array(Int64) DEFAULT arrayMap(x -> x.y + `x.y` * 1000, arr)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_field_pair RENAME COLUMN `x.y` TO `x.z`; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_field_pair (`x.y`, arr) VALUES (7, [(1), (2)]);
SELECT 'field pair', c2 FROM t_field_pair;
DROP TABLE t_field_pair;

-- `share_nested_offsets = 0` allows renaming a column to a dotted name.
DROP TABLE IF EXISTS t_field_new;
CREATE TABLE t_field_new (c9 Int64, arr Array(Tuple(z Int64)), c2 Array(Int64) DEFAULT arrayMap(x -> c9 + x.z, arr)) ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
ALTER TABLE t_field_new RENAME COLUMN c9 TO `x.z`; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_field_new (c9, arr) VALUES (100, [(1), (2)]);
SELECT 'field new', c2 FROM t_field_new;
DROP TABLE t_field_new;

-- A lambda parameter with the new name, in each kind of expression.
DROP TABLE IF EXISTS t_lambda;
CREATE TABLE t_lambda
(
    x Int64,
    arr Array(Int64),
    d Date,
    c2 Array(Int64) DEFAULT arrayMap(p0 -> p0 + x, arr),
    c3 Int64 TTL d + toIntervalDay(arraySum(arrayMap(p1 -> p1 + x, arr))),
    CONSTRAINT k CHECK arrayAll(p3 -> p3 < x, arr),
    INDEX i arraySum(arrayMap(p4 -> p4 + x, arr)) TYPE minmax
)
ENGINE = MergeTree ORDER BY tuple() TTL d + toIntervalDay(arraySum(arrayMap(p2 -> p2 + x, arr)));
INSERT INTO t_lambda (x, arr, d, c3) VALUES (100, [1, 2], '2100-01-01', 5);
ALTER TABLE t_lambda RENAME COLUMN x TO p0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_lambda RENAME COLUMN x TO p1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_lambda RENAME COLUMN x TO p2; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_lambda RENAME COLUMN x TO p3; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_lambda RENAME COLUMN x TO p4; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_lambda (x, arr, d, c3) VALUES (100, [1, 2], '2100-01-01', 5);
SELECT 'lambda', c2, c3 FROM t_lambda;
DROP TABLE t_lambda;

-- Outside column defaults an alias inside a lambda is visible in the whole expression.
DROP TABLE IF EXISTS t_constraint_lambda_alias;
CREATE TABLE t_constraint_lambda_alias (a Int64, arr Array(Int64), CONSTRAINT k CHECK arrayAll(x -> (x + 1 AS b) > 0, arr) AND (a > 0)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_constraint_lambda_alias RENAME COLUMN a TO b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_constraint_lambda_alias;

-- An alias in a TTL GROUP BY SET expression.
DROP TABLE IF EXISTS t_ttl_set_alias;
CREATE TABLE t_ttl_set_alias (d Date, key UInt64, c0 UInt64, c1 UInt64, value UInt64) ENGINE = MergeTree ORDER BY key TTL d + INTERVAL 1 YEAR GROUP BY key SET value = any((c0 AS c1) + c1);
ALTER TABLE t_ttl_set_alias RENAME COLUMN c1 TO v; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_ttl_set_alias;

-- The options of an index type are not column expressions.
DROP TABLE IF EXISTS t_index_options;
CREATE TABLE t_index_options (keyword String, s String, INDEX i s TYPE text(tokenizer = keyword, preprocessor = lower(s), postprocessor = upper(s))) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_index_options RENAME COLUMN keyword TO array;
ALTER TABLE t_index_options RENAME COLUMN s TO s2;
SELECT 'index options', expr, type_full FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_options';
DROP TABLE t_index_options;

-- Lambda parameters and the names they bind are not renamed.
DROP TABLE IF EXISTS t_nested_lambda;
CREATE TABLE t_nested_lambda (x Int64, arr Array(Int64), c2 Array(Array(Int64)) DEFAULT arrayMap(y -> arrayMap(x -> x + y, arr), arr)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested_lambda (x, arr) VALUES (5, [1, 2]);
ALTER TABLE t_nested_lambda RENAME COLUMN x TO y;
INSERT INTO t_nested_lambda (y, arr) VALUES (5, [1, 2]);
SELECT 'nested lambda', c2 FROM t_nested_lambda;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_nested_lambda' AND name = 'c2';
DROP TABLE t_nested_lambda;

DROP TABLE IF EXISTS t_shadow;
CREATE TABLE t_shadow (c1 Int64, arr Array(Int64), c2 Int64 DEFAULT arrayMap(c1 -> c1 * 2, arr)[1] + c1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow (c1, arr) VALUES (10, [1]);
ALTER TABLE t_shadow RENAME COLUMN c1 TO v;
INSERT INTO t_shadow (v, arr) VALUES (10, [1]);
SELECT 'shadow', c2 FROM t_shadow;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_shadow' AND name = 'c2';
DROP TABLE t_shadow;

DROP TABLE IF EXISTS t_param_field;
CREATE TABLE t_param_field (`x.y` Int64, arr Array(Tuple(y Int64)), c2 Array(Int64) DEFAULT arrayMap(x -> x.y, arr)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_param_field (`x.y`, arr) VALUES (100, [(1), (2)]);
ALTER TABLE t_param_field RENAME COLUMN `x.y` TO `x.z`;
INSERT INTO t_param_field (`x.z`, arr) VALUES (100, [(1), (2)]);
SELECT 'param field', c2 FROM t_param_field;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_param_field' AND name = 'c2';
DROP TABLE t_param_field;

DROP TABLE IF EXISTS t_quoted;
CREATE TABLE t_quoted (`x.y` Int64, arr Array(Tuple(y Int64)), c2 Array(Int64) DEFAULT arrayMap(x -> `x.y` * 1000, arr)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_quoted (`x.y`, arr) VALUES (7, [(1), (2)]);
ALTER TABLE t_quoted RENAME COLUMN `x.y` TO `x.z`;
INSERT INTO t_quoted (`x.z`, arr) VALUES (7, [(1), (2)]);
SELECT 'quoted', c2 FROM t_quoted;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_quoted' AND name = 'c2';
DROP TABLE t_quoted;

DROP TABLE IF EXISTS t_to_field_name;
CREATE TABLE t_to_field_name (a Int64, arr Array(Tuple(y Int64)), c2 Array(Int64) DEFAULT arrayMap(x -> a + x.y, arr)) ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_to_field_name (a, arr) VALUES (10, [(1), (2)]);
ALTER TABLE t_to_field_name RENAME COLUMN a TO `x.z`;
INSERT INTO t_to_field_name (`x.z`, arr) VALUES (10, [(1), (2)]);
SELECT 'to field name', c2 FROM t_to_field_name;
DROP TABLE t_to_field_name;

-- Identifiers and aliases inside a subquery belong to the subquery.
DROP TABLE IF EXISTS t_subquery_alias;
CREATE TABLE t_subquery_alias (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE x < (SELECT 1 AS u);
ALTER TABLE t_subquery_alias RENAME COLUMN x TO u;
SELECT 'subquery alias', position(create_table_query, 'WHERE u < (SELECT 1 AS u)') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_subquery_alias';
DROP TABLE t_subquery_alias;

DROP TABLE IF EXISTS t_subquery_local;
CREATE TABLE t_subquery_local (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE x < (SELECT max(x) FROM (SELECT 100 AS x));
ALTER TABLE t_subquery_local RENAME COLUMN x TO u;
SELECT 'subquery local', position(create_table_query, 'WHERE u < (SELECT max(x) FROM (SELECT 100 AS x))') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_subquery_local';
DROP TABLE t_subquery_local;

DROP TABLE IF EXISTS t_subquery_outer_alias;
CREATE TABLE t_subquery_outer_alias (d DateTime, c0 UInt64, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE (c0 AS u) > 0 AND c0 < (SELECT max(x) FROM (SELECT 100 AS x));
ALTER TABLE t_subquery_outer_alias RENAME COLUMN x TO u;
SELECT 'subquery outer alias', position(create_table_query, 'SELECT max(x) FROM (SELECT 100 AS x)') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_subquery_outer_alias';
DROP TABLE t_subquery_outer_alias;

-- A TTL GROUP BY SET assignment, target and expression, is renamed like the other expressions.
DROP TABLE IF EXISTS t_ttl_set;
CREATE TABLE t_ttl_set (d Date, key UInt64, c1 UInt64, value UInt64) ENGINE = MergeTree ORDER BY key TTL d + INTERVAL 1 YEAR GROUP BY key SET value = max(c1);
ALTER TABLE t_ttl_set RENAME COLUMN c1 TO v;
SELECT 'ttl set', position(create_table_query, 'SET value = max(v)') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_set';
DROP TABLE t_ttl_set;

DROP TABLE IF EXISTS t_ttl_set_target;
CREATE TABLE t_ttl_set_target (d Date, key UInt64, c1 UInt64, value UInt64) ENGINE = MergeTree ORDER BY key TTL d + INTERVAL 1 YEAR GROUP BY key SET value = max(c1);
ALTER TABLE t_ttl_set_target RENAME COLUMN value TO v;
SELECT 'ttl set target', position(create_table_query, 'SET v = max(c1)') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_set_target';
DROP TABLE t_ttl_set_target;

-- Renames that keep every reference bound to the same column.
DROP TABLE IF EXISTS t_self_alias;
CREATE TABLE t_self_alias (c1 Int64, c2 Int64 DEFAULT (c1 AS u) + u) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_self_alias (c1) VALUES (10);
ALTER TABLE t_self_alias RENAME COLUMN c1 TO u;
INSERT INTO t_self_alias (u) VALUES (10);
SELECT 'self alias', c2 FROM t_self_alias;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_self_alias' AND name = 'c2';
DROP TABLE t_self_alias;

DROP TABLE IF EXISTS t_self_alias_same_name;
CREATE TABLE t_self_alias_same_name (c1 Int64, c2 Int64 DEFAULT (c1 AS c1) + c1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_self_alias_same_name (c1) VALUES (10);
ALTER TABLE t_self_alias_same_name RENAME COLUMN c1 TO v;
INSERT INTO t_self_alias_same_name (v) VALUES (10);
SELECT 'self alias same name', c2 FROM t_self_alias_same_name;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_self_alias_same_name' AND name = 'c2';
DROP TABLE t_self_alias_same_name;

DROP TABLE IF EXISTS t_own_definition;
CREATE TABLE t_own_definition (a Int64, c2 Int64 DEFAULT (a + 1 AS b)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_own_definition RENAME COLUMN a TO b;
INSERT INTO t_own_definition (b) SELECT 5;
SELECT 'own definition', c2 FROM t_own_definition;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_own_definition' AND name = 'c2';
DROP TABLE t_own_definition;

DROP TABLE IF EXISTS t_constraint_own_definition;
CREATE TABLE t_constraint_own_definition (a Int64, CONSTRAINT k CHECK (a + 1 AS b) > 0) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_constraint_own_definition RENAME COLUMN a TO b;
INSERT INTO t_constraint_own_definition (b) VALUES (5);
INSERT INTO t_constraint_own_definition (b) VALUES (-5); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'constraint own definition', b FROM t_constraint_own_definition;
DROP TABLE t_constraint_own_definition;

DROP TABLE IF EXISTS t_outside_lambda;
CREATE TABLE t_outside_lambda (x Int64, arr Array(Int64), c2 Int64 DEFAULT arrayMap(c1 -> c1 + 1, arr)[1] + x) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_outside_lambda RENAME COLUMN x TO c1;
INSERT INTO t_outside_lambda (c1, arr) VALUES (100, [1]);
SELECT 'outside lambda', c2 FROM t_outside_lambda;
DROP TABLE t_outside_lambda;

DROP TABLE IF EXISTS t_unused_alias;
CREATE TABLE t_unused_alias (c0 Int64, c1 Int64, c2 Int64 DEFAULT (c0 AS u) + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_unused_alias (c0, c1) VALUES (1, 10);
ALTER TABLE t_unused_alias RENAME COLUMN c1 TO u;
INSERT INTO t_unused_alias (c0, u) VALUES (1, 10);
SELECT 'unused alias', c2 FROM t_unused_alias;
DROP TABLE t_unused_alias;

DROP TABLE IF EXISTS t_lambda_local_alias;
CREATE TABLE t_lambda_local_alias (a Int64, arr Array(Int64), c2 Int64 DEFAULT arrayMap(x -> (x + 1 AS b) * b, arr)[1] + a) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_local_alias (a, arr) VALUES (100, [1]);
ALTER TABLE t_lambda_local_alias RENAME COLUMN a TO b;
INSERT INTO t_lambda_local_alias (b, arr) VALUES (100, [1]);
INSERT INTO t_lambda_local_alias (b, arr) SETTINGS prefer_column_name_to_alias = 1 VALUES (100, [1]);
SELECT 'lambda local alias', c2 FROM t_lambda_local_alias;
DROP TABLE t_lambda_local_alias;

DROP TABLE IF EXISTS t_lambda_node_alias;
CREATE TABLE t_lambda_node_alias (a Int64, arr Array(Int64), c2 Int64 DEFAULT arrayMap((x -> x + 1) AS b, arr)[1] + a) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_node_alias (a, arr) VALUES (10, [1]);
ALTER TABLE t_lambda_node_alias RENAME COLUMN a TO b;
INSERT INTO t_lambda_node_alias (b, arr) VALUES (10, [1]);
SELECT 'lambda node alias', c2 FROM t_lambda_node_alias;
DROP TABLE t_lambda_node_alias;

DROP TABLE IF EXISTS t_separate_scopes;
CREATE TABLE t_separate_scopes (c0 Int64, x Int64, d Date, c2 Int64 DEFAULT (c0 AS u) + 1, c3 Int64 TTL d + toIntervalDay(x)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_separate_scopes RENAME COLUMN x TO u;
INSERT INTO t_separate_scopes (c0, u, d, c3) VALUES (1, 10, '2100-01-01', 7);
SELECT 'separate scopes', c2, c3 FROM t_separate_scopes;
DROP TABLE t_separate_scopes;

DROP TABLE IF EXISTS t_other_name;
CREATE TABLE t_other_name (c0 Int64, c1 Int64, c2 Int64 DEFAULT (c0 AS u) + c1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_other_name (c0, c1) VALUES (1, 10);
ALTER TABLE t_other_name RENAME COLUMN c1 TO z;
INSERT INTO t_other_name (c0, z) VALUES (1, 10);
SELECT 'other name', c2 FROM t_other_name;
DROP TABLE t_other_name;
