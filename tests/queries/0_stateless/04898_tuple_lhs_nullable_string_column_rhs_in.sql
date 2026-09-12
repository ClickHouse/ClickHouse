-- A NULL value of a nullable string column on the right of IN with a tuple LHS is an absent
-- set element, like the constant analog `(1, 2) IN (CAST(NULL AS Nullable(String)))` and the
-- `Nullable(Nothing)` column of 04870. It used to reach the cast to the non-Nullable tuple
-- type and fail with CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN.
-- The left-hand tuples below are the DEFAULT value of their type on purpose: the value
-- substituted for the NULL before the cast is that default, so an LHS that differs from it
-- would return the right answer even without the guard that makes the substitute unreachable.
-- Only the analyzer resolves a bare source column on the right of IN; the old analyzer binds
-- it as a table name (see 04234), so there is no enable_analyzer = 0 section.

SET enable_analyzer = 1;

-- { echoOn }

SELECT (0, 0) IN (rhs), (0, 0) NOT IN (rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs);
SELECT nullIn((0, 0), rhs), notNullIn((0, 0), rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs);
SELECT (0, 0) IN (rhs), (0, 0) NOT IN (rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs) SETTINGS transform_null_in = 1;
SELECT ('', '') IN (rhs), ('', '') NOT IN (rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs);
SELECT (0, 0) IN (rhs) FROM (SELECT materialize(toLowCardinality(CAST(NULL AS Nullable(String)))) AS rhs);
SELECT (x, y) IN (rhs) FROM (SELECT 0 AS x, 0 AS y, materialize(CAST(NULL AS Nullable(String))) AS rhs);
SELECT (0, 0) IN (rhs) FROM (SELECT CAST(NULL AS Nullable(String)) AS rhs);
SELECT (toNullable(0), 0) IN (rhs), (toNullable(0), 0) NOT IN (rhs), toTypeName((toNullable(0), 0) IN (rhs)) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs);

-- The substitution before the cast, not lazy `if` evaluation, is what keeps the cast total:
-- these repeat the NULL arms with short-circuiting off.
SELECT (0, 0) IN (rhs), (0, 0) NOT IN (rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs) SETTINGS short_circuit_function_evaluation = 'disable';
SELECT nullIn((0, 0), rhs), notNullIn((0, 0), rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs) SETTINGS short_circuit_function_evaluation = 'disable';
SELECT (0, 0) IN (rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs) SETTINGS short_circuit_function_evaluation = 'force_enable';

-- A present value is still parsed into the tuple type and compared, NULL or not.
SELECT (1, 2) IN (rhs), (1, 2) NOT IN (rhs) FROM (SELECT materialize(CAST('(1,2)' AS Nullable(String))) AS rhs);
SELECT (1, 2) IN (rhs) FROM (SELECT materialize(CAST('(9,9)' AS Nullable(String))) AS rhs);
SELECT nullIn((1, 2), rhs), notNullIn((1, 2), rhs) FROM (SELECT materialize(CAST('(1,2)' AS Nullable(String))) AS rhs);
SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize(CAST('(''a'',''b'')' AS Nullable(String))) AS rhs);
SELECT (1, 2) IN (rhs) FROM (SELECT materialize(toLowCardinality(CAST('(1,2)' AS Nullable(String)))) AS rhs);

-- NULL and present values mix per row within one column.
SELECT rhs, (0, 0) IN (rhs), (0, 0) NOT IN (rhs), nullIn((0, 0), rhs), notNullIn((0, 0), rhs)
FROM (SELECT arrayJoin([CAST(NULL AS Nullable(String)), '(0,0)', '(9,9)']) AS rhs) ORDER BY rhs;

-- A present but non-parseable value keeps raising the parsing error of the plain String RHS
-- pinned by 04871.
SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize(CAST('not a tuple' AS Nullable(String))) AS rhs); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- A Nullable tuple LHS keeps its three-valued result: the cast target is nullable there, so
-- the NULL reaches the comparison instead of being reported absent.
SELECT CAST(NULL AS Nullable(Tuple(UInt8, UInt8))) IN (rhs), nullIn(CAST(NULL AS Nullable(Tuple(UInt8, UInt8))), rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(String))) AS rhs) SETTINGS enable_nullable_tuple_type = 1;

-- A tuple with a bare `Nothing` element has no default value to substitute, so it keeps the
-- plain cast and stays resolvable. The element may sit at the top level or nested inside
-- another tuple, and the tuple may come from a column type or from an expression.
CREATE TABLE t_nothing (lhs Tuple(UInt8, Nothing), rhs Nullable(String)) ENGINE = Memory;
SELECT lhs IN (rhs), lhs NOT IN (rhs) FROM t_nothing;
SELECT (1, arrayJoin([])) IN (rhs), (1, arrayJoin([])) NOT IN (rhs) FROM t_nothing;
CREATE TABLE t_nothing_nested (lhs Tuple(UInt8, Tuple(Nothing)), rhs Nullable(String)) ENGINE = Memory;
SELECT lhs IN (rhs), lhs NOT IN (rhs) FROM t_nothing_nested;

-- Casting a FixedString to a tuple is unsupported for either nullability, so both keep the
-- same error rather than the nullable one gaining support.
SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize(CAST(NULL AS Nullable(FixedString(9)))) AS rhs); -- { serverError TYPE_MISMATCH }
SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize(CAST('(''a'',''b'')' AS FixedString(11))) AS rhs); -- { serverError TYPE_MISMATCH }
