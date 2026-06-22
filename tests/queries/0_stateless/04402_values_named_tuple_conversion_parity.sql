-- Tests that the interpreted VALUES path converts named tuples consistently with INSERT ... SELECT:
--  - nested named tuples are reordered by name recursively;
--  - input_format_null_as_default fills defaults against the destination elements after reordering;
--  - the named-tuple insert guard applies to Dynamic/JSON destinations too.

SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;
SET input_format_values_interpret_expressions = 1;

-- Blocker 1: a nested named tuple must be reordered by name (recursively), exactly like INSERT ... SELECT.
DROP TABLE IF EXISTS t_values_nested;
CREATE TABLE t_values_nested (x Tuple(n Tuple(a Int32, b Int32))) ENGINE = Memory;
INSERT INTO t_values_nested VALUES (tuple('n')(tuple('b', 'a')(1, 2)));
INSERT INTO t_values_nested SELECT tuple('n')(tuple('b', 'a')(1, 2));
-- Both rows must be identical (a = 2, b = 1); the nested tuple is matched by name, not by position.
SELECT x.n.a, x.n.b FROM t_values_nested;
DROP TABLE t_values_nested;

-- Blocker 2: with input_format_null_as_default, defaults must be applied to the destination elements
-- after the named tuple has been reordered, not before.
DROP TABLE IF EXISTS t_values_null_default;
CREATE TABLE t_values_null_default (t Tuple(a UInt8, b String)) ENGINE = Memory;
INSERT INTO t_values_null_default VALUES (tuple('b', 'a')('x', NULL)) SETTINGS input_format_null_as_default = 1;
-- `a` is NULL in the source and must become the UInt8 default 0; `b` must keep 'x'.
SELECT t.a, t.b FROM t_values_null_default;
DROP TABLE t_values_null_default;

-- Blocker 3: the named-tuple insert guard must also apply to Dynamic/JSON destinations in VALUES.
DROP TABLE IF EXISTS t_values_dynamic;
CREATE TABLE t_values_dynamic (t Tuple(data Dynamic)) ENGINE = Memory;
-- Matching element name works.
INSERT INTO t_values_dynamic VALUES (tuple('data')(42));
SELECT t.data FROM t_values_dynamic;
-- A differently named source field would silently drop the value into a default. It must throw,
-- consistently with INSERT ... SELECT, instead of being permissively cast with a null context.
INSERT INTO t_values_dynamic VALUES (tuple('val')(42)); -- { serverError CANNOT_CONVERT_TYPE }
INSERT INTO t_values_dynamic SELECT tuple('val')(42); -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t_values_dynamic;
