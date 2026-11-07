-- Tags: no-fasttest

-- Regression test for STID 3344-4a3c
-- (Logical error: 'Unexpected return type from lessOrEquals. Expected UInt8. Got Const(Nullable(UInt8))').
--
-- When comparing a nested tuple containing `Nullable` elements with a `String` literal
-- (e.g. `Tuple(Tuple(Nullable(String))) <= '((NULL))'`), the type-inference loop in
-- `FunctionComparison::getReturnTypeImpl` only recursed for the both-tuples case.
-- For the tuple-vs-string case it inspected the immediate element type only, so a
-- `Tuple(Nullable(...))` element was not detected as nullable-producing and the
-- comparison was inferred as plain `UInt8`. The runtime, which converts the string
-- to a tuple value and compares element-wise, produced `Nullable(UInt8)`, tripping
-- the analyzer's constant-folding type-match assertion at
-- `src/Analyzer/Resolve/resolveFunction.cpp`.

SET enable_analyzer = 1;

-- Minimal reproducers — must NOT crash and must return `Nullable(UInt8)`.
SELECT toTypeName(tuple(tuple(toNullable('x'))) <= '((NULL))');
SELECT tuple(tuple(toNullable('x'))) <= '((NULL))';

SELECT toTypeName(tuple(1, tuple(toNullable('x'), 2)) <= '(1, (NULL, 2))');
SELECT tuple(1, tuple(toNullable('x'), 2)) <= '(1, (NULL, 2))';

-- Use inside a `PREWHERE` filter — this is the shape that surfaced the bug in production
-- CI on PR #100300 (analyzer constant-folding path).
DROP TABLE IF EXISTS t64;
CREATE TABLE t64 (`t_i8` Int8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t64 VALUES (0), (1), (-1);

SELECT *
FROM t64
PREWHERE
    tuple(2147483648, (SELECT materialize(toNullable('y')), 2)) <= '(1, (NULL, 2))'
FORMAT Null;

DROP TABLE t64;

-- Sanity: existing well-formed cases keep working.
SELECT toTypeName(tuple(1) <= '(2)');
SELECT toTypeName(tuple(toNullable(1)) <= '(NULL)');

-- String on the LEFT, nested-`Nullable` tuple on the RIGHT — symmetric path.
SELECT toTypeName('((NULL))' <= tuple(tuple(toNullable('x'))));
SELECT '((NULL))' <= tuple(tuple(toNullable('x')));

-- Both sides are tuples — the previously-existing recursion path. Must keep working.
SELECT toTypeName(tuple(tuple(toNullable('x'))) <= tuple(tuple(toNullable('y'))));
SELECT tuple(tuple(toNullable('x'))) <= tuple(tuple(toNullable('y')));
