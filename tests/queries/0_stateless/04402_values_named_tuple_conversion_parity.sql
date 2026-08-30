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
INSERT INTO t_values_null_default SETTINGS input_format_null_as_default = 1 VALUES (tuple('b', 'a')('x', NULL));
-- `a` is NULL in the source and must become the UInt8 default 0; `b` must keep 'x'.
SELECT t.a, t.b FROM t_values_null_default;
DROP TABLE t_values_null_default;

-- Blocker 3: the named-tuple insert guard must also apply to Dynamic/JSON destinations in VALUES.
DROP TABLE IF EXISTS t_values_dynamic;
CREATE TABLE t_values_dynamic (t Tuple(data Dynamic, other Int32)) ENGINE = Memory;
-- Matching element names work.
INSERT INTO t_values_dynamic VALUES (tuple('data', 'other')(42, 1));
-- A tuple whose element names are disjoint with the destination is converted positionally
-- and keeps the data (see 05026_named_tuple_cast_unambiguous), on both paths.
INSERT INTO t_values_dynamic VALUES (tuple('a', 'b')(43, 2));
INSERT INTO t_values_dynamic SELECT tuple('a', 'b')(44, 3);
SELECT t.data, t.other FROM t_values_dynamic ORDER BY t.other;
-- With a common element name the tuples are matched by name, and a source field without a
-- counterpart would silently drop into a default. It must throw, consistently with
-- INSERT ... SELECT, instead of being permissively cast with a null context.
-- The inline VALUES data is parsed in the client (see ClientBase::sendDataFrom), so the guard is
-- normally applied there and the error is reported as a client error; INSERT ... SELECT is converted
-- on the server and reports a server error. With async_insert the client sends the block unconverted
-- and the server-side guard rejects it during WaitForAsyncInsert, so a server error is reported
-- instead. Either way the conversion must be rejected.
INSERT INTO t_values_dynamic VALUES (tuple('data', 'val')(45, 7)); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_dynamic SELECT tuple('data', 'val')(45, 7); -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t_values_dynamic;
