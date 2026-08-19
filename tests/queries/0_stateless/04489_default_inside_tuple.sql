-- DEFAULT expressions inside Tuple data types are pulled up to the column level.
-- https://github.com/ClickHouse/ClickHouse/issues/2797

DROP TABLE IF EXISTS t_default_in_tuple;
DROP TABLE IF EXISTS t_nested_tuple;
DROP TABLE IF EXISTS t_ref;
DROP TABLE IF EXISTS t_expr;

SELECT '-- basic';
CREATE TABLE t_default_in_tuple
(
    id UInt8,
    c Tuple(a UInt8, s String DEFAULT 'Hello')
)
ENGINE = MergeTree ORDER BY id;

-- The stored type is normalized: no DEFAULT inside the Tuple, a pulled-up column default instead.
SELECT name, type,
    coalesce(nullIf(default_kind, ''), '<none>'),
    coalesce(nullIf(default_expression, ''), '<none>')
FROM system.columns
WHERE database = currentDatabase() AND table = 't_default_in_tuple'
ORDER BY name;

INSERT INTO t_default_in_tuple (id) VALUES (1);
SELECT id, c FROM t_default_in_tuple;

SELECT '-- nested tuple';
CREATE TABLE t_nested_tuple
(
    id UInt8,
    c Tuple(a UInt8, t Tuple(x String DEFAULT 'q', y UInt8))
)
ENGINE = MergeTree ORDER BY id;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_nested_tuple' AND name = 'c';
INSERT INTO t_nested_tuple (id, c) VALUES (1, (5, ('z', 1)));
INSERT INTO t_nested_tuple (id) VALUES (2);
SELECT id, c FROM t_nested_tuple ORDER BY id;

SELECT '-- default referencing a column';
CREATE TABLE t_ref
(
    x UInt8,
    c Tuple(a UInt8 DEFAULT x, s String)
)
ENGINE = MergeTree ORDER BY x;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_ref' AND name = 'c';
INSERT INTO t_ref (x) VALUES (7);
SELECT c FROM t_ref;

SELECT '-- constant expression default';
CREATE TABLE t_expr
(
    id UInt8,
    c Tuple(a UInt8 DEFAULT 1 + 2, s String DEFAULT upper('hi'))
)
ENGINE = MergeTree ORDER BY id;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_expr' AND name = 'c';
INSERT INTO t_expr (id) VALUES (1);
SELECT c FROM t_expr;

SELECT '-- errors';
-- Referencing a sibling element (ambiguous with a column of the same name) is rejected.
CREATE TABLE t_amb (a UInt8, c Tuple(a UInt8, b UInt8 DEFAULT a)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- Referencing another element of the same tuple is rejected.
CREATE TABLE t_sib (c Tuple(a UInt8, b UInt8 DEFAULT a)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- A column-level default together with DEFAULTs inside the type is rejected.
CREATE TABLE t_conflict (c Tuple(a UInt8 DEFAULT 1) DEFAULT (2)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- DEFAULT inside Nested is not supported.
CREATE TABLE t_nested (n Nested(x String, y UInt8 DEFAULT 5)) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
-- DEFAULT inside Array is not supported.
CREATE TABLE t_array (c Array(Tuple(x UInt8 DEFAULT 5))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
-- Building a data type with a DEFAULT directly is rejected.
SELECT defaultValueOfTypeName('Tuple(a UInt8 DEFAULT 5)'); -- { serverError BAD_ARGUMENTS }
SELECT CAST((1, 2), 'Tuple(a UInt8, b UInt8 DEFAULT 5)'); -- { serverError BAD_ARGUMENTS }

SELECT '-- default null';
CREATE TABLE t_default_null
(
    id UInt8,
    c Tuple(a UInt8 DEFAULT NULL, b String)
)
ENGINE = Memory;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_default_null' AND name = 'c';
INSERT INTO t_default_null (id) VALUES (1);
SELECT c FROM t_default_null;

-- The same normalization applies to ALTER, not only CREATE.
SELECT '-- alter add column';
DROP TABLE IF EXISTS t_alter_add;
CREATE TABLE t_alter_add (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_add ADD COLUMN c Tuple(a UInt8, s String DEFAULT 'Hi');
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_add' AND name = 'c';
INSERT INTO t_alter_add (id) VALUES (1);
SELECT id, c FROM t_alter_add;

SELECT '-- alter modify column';
DROP TABLE IF EXISTS t_alter_modify;
CREATE TABLE t_alter_modify (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_modify MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT 42);
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_modify' AND name = 'c';
INSERT INTO t_alter_modify (id) VALUES (1);
SELECT id, c FROM t_alter_modify;

SELECT '-- alter errors';
-- DEFAULT inside Nested is not supported on ALTER either.
ALTER TABLE t_alter_add ADD COLUMN n Nested(x String, y UInt8 DEFAULT 5); -- { serverError NOT_IMPLEMENTED }
-- DEFAULT inside Array is not supported on ALTER either.
ALTER TABLE t_alter_add MODIFY COLUMN c Array(Tuple(x UInt8 DEFAULT 5)); -- { serverError NOT_IMPLEMENTED }

SELECT '-- old distributed DDL format';
SET distributed_ddl_entry_format_version = 2;
DROP TABLE IF EXISTS t_default_in_tuple_cluster ON CLUSTER test_shard_localhost FORMAT Null;
CREATE TABLE t_default_in_tuple_cluster ON CLUSTER test_shard_localhost
(
    id UInt8,
    c Tuple(a UInt8, s String DEFAULT 'Hello')
)
ENGINE = MergeTree ORDER BY id FORMAT Null;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_default_in_tuple_cluster' AND name = 'c';
DROP TABLE t_default_in_tuple_cluster ON CLUSTER test_shard_localhost FORMAT Null;
SET distributed_ddl_entry_format_version = 0;

SELECT '-- nullable tuple';
-- Nullable is a transparent wrapper around a Tuple, so a DEFAULT inside Nullable(Tuple(...)) is
-- pulled up as the same column-level tuple(...) default and cast to the nullable tuple type.
SET enable_nullable_tuple_type = 1;
CREATE TABLE t_nullable
(
    id UInt8,
    c Nullable(Tuple(a UInt8, s String DEFAULT 'Hi'))
)
ENGINE = MergeTree ORDER BY id;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_nullable' AND name = 'c';
INSERT INTO t_nullable (id) VALUES (1);
SELECT id, c FROM t_nullable;

SELECT '-- variant alternative';
-- A value of an alternative of a Variant is a valid value of the whole Variant, so a DEFAULT inside
-- a Tuple alternative is pulled up as the default of the column, cast to the type of the alternative.
CREATE TABLE t_variant
(
    id UInt8,
    c Variant(UInt64, Tuple(a UInt32, b UInt32 DEFAULT 5))
)
ENGINE = MergeTree ORDER BY id;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_variant' AND name = 'c';
INSERT INTO t_variant (id) VALUES (1);
INSERT INTO t_variant (id, c) VALUES (2, 7);
SELECT id, c, variantType(c) FROM t_variant ORDER BY id;
-- A column has a single default value, so at most one alternative may define one.
CREATE TABLE t_variant_two (c Variant(Tuple(a UInt32 DEFAULT 1), Tuple(b String, c String DEFAULT 'x'))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- An unsupported wrapper inside a Variant is still rejected.
CREATE TABLE t_variant_array (c Variant(UInt64, Array(Tuple(a UInt32 DEFAULT 1)))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }

SELECT '-- simple aggregate function';
-- SimpleAggregateFunction stores plain values of its storage type (the first type argument), so a
-- DEFAULT inside `SimpleAggregateFunction(f, Tuple(...))` is pulled up as the column-level default.
CREATE TABLE t_saf
(
    id UInt8,
    c SimpleAggregateFunction(any, Tuple(a UInt8 DEFAULT 1, b UInt8))
)
ENGINE = AggregatingMergeTree ORDER BY id;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_saf' AND name = 'c';
INSERT INTO t_saf (id) VALUES (1);
INSERT INTO t_saf (id, c) VALUES (2, (3, 4));
SELECT id, c FROM t_saf ORDER BY id;
ALTER TABLE t_saf ADD COLUMN d SimpleAggregateFunction(max, Tuple(a UInt8, s String DEFAULT 'Hi'));
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_saf' AND name = 'd';
SELECT id, d FROM t_saf ORDER BY id;
ALTER TABLE t_saf MODIFY COLUMN d SimpleAggregateFunction(max, Tuple(a UInt16 DEFAULT 9, s String));
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_saf' AND name = 'd';
-- A SimpleAggregateFunction wrapping a tuple with a default may itself be an element of a tuple.
CREATE TABLE t_saf_nested (id UInt8, c Tuple(x SimpleAggregateFunction(any, Tuple(a UInt8 DEFAULT 7)), y String)) ENGINE = MergeTree ORDER BY id;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_saf_nested' AND name = 'c';
INSERT INTO t_saf_nested (id) VALUES (1);
SELECT id, c FROM t_saf_nested;
-- The ambiguity check applies inside the wrapper as well.
CREATE TABLE t_saf_ambiguous (c SimpleAggregateFunction(any, Tuple(a UInt8 DEFAULT b, b UInt8))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- An unsupported wrapper inside the storage type is still rejected.
CREATE TABLE t_saf_array (c SimpleAggregateFunction(groupArrayArray, Array(Tuple(a UInt8 DEFAULT 1)))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }

SELECT '-- lambda parameter shadowing an element name';
-- A lambda parameter is a scoped local variable, not a reference to a tuple element or a column, so
-- a parameter named like an element does not make the default ambiguous.
CREATE TABLE t_lambda
(
    x UInt8,
    c Tuple(x UInt8, y Array(UInt8) DEFAULT arrayMap(x -> x + 1, [1, 2, 3]))
)
ENGINE = MergeTree ORDER BY x;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_lambda' AND name = 'c';
INSERT INTO t_lambda (x) VALUES (7);
SELECT c FROM t_lambda;

SELECT '-- lambda body free identifier colliding with an element name';
-- A free identifier in a lambda body (not bound by the lambda) that collides with an element name is
-- still rejected as ambiguous.
CREATE TABLE t_lambda_free (c Tuple(a UInt8, y Array(UInt8) DEFAULT arrayMap(z -> z + a, [1]))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

SELECT '-- element name scope';
-- Element names are only ambiguous where they are visible. An element of an unrelated nested tuple
-- that reuses the name of a table column does not make a default ambiguous: after the pull-up, `x`
-- in the default below can only resolve to the table column.
CREATE TABLE t_scope
(
    x UInt8,
    c Tuple(a UInt8 DEFAULT x, nested Tuple(x String, y UInt8))
)
ENGINE = MergeTree ORDER BY x;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_scope' AND name = 'c';
INSERT INTO t_scope (x) VALUES (5);
SELECT c FROM t_scope;
-- A reference to an element of an enclosing tuple is still ambiguous.
CREATE TABLE t_scope_outer (a UInt8, c Tuple(a UInt8, n Tuple(b UInt8 DEFAULT a))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_default_in_tuple;
DROP TABLE t_nested_tuple;
DROP TABLE t_ref;
DROP TABLE t_expr;
DROP TABLE t_default_null;
DROP TABLE t_alter_add;
DROP TABLE t_alter_modify;
DROP TABLE t_nullable;
DROP TABLE t_variant;
DROP TABLE t_saf;
DROP TABLE t_saf_nested;
DROP TABLE t_lambda;
DROP TABLE t_scope;
