SET enable_analyzer = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt32) ENGINE = MergeTree ORDER BY tuple();

-- Original reproducer: should not throw a logical error exception
SELECT * FROM t PREWHERE NULL + arrayMap(x -> *, [1]);

-- Test with actual data: PREWHERE and WHERE should produce the same results
INSERT INTO t VALUES (100)(200);

SELECT 'PREWHERE arrayMap(x -> *, [1])';
SELECT * FROM t PREWHERE arrayMap(x -> *, [1])[1] > 50;

SELECT 'WHERE arrayMap(x -> *, [1])';
SELECT * FROM t WHERE arrayMap(x -> *, [1])[1] > 50;

SELECT 'PREWHERE arrayMap(x -> t.x, [1])';
SELECT * FROM t PREWHERE arrayMap(x -> t.x, [1])[1] > 50;

SELECT 'WHERE arrayMap(x -> t.x, [1])';
SELECT * FROM t WHERE arrayMap(x -> t.x, [1])[1] > 50;

-- Non-colliding lambda arg name for comparison
SELECT 'PREWHERE arrayMap(y -> *, [1])';
SELECT * FROM t PREWHERE arrayMap(y -> *, [1])[1] > 50;

-- Sibling lambdas: first captures table column, second uses lambda argument.
-- The second lambda must NOT be confused by the first lambda having added x to the outer scope.
-- Output the computed value directly to distinguish correct binding (lambda arg 1 → 1+1=2)
-- from incorrect binding (table column 100/200 → 101/201).
SELECT 'sibling lambdas: capture + argument';
SELECT arrayMap(x -> x + 1, [1])[1] FROM t PREWHERE arrayMap(x -> *, [1])[1] + arrayMap(x -> x + 1, [1])[1] > 50;

SELECT 'sibling lambdas reversed';
SELECT arrayMap(x -> x + 1, [1])[1] FROM t PREWHERE arrayMap(x -> x + 1, [1])[1] + arrayMap(x -> *, [1])[1] > 50;

-- Nested lambdas: inner lambda references table column x, which collides
-- with the outer lambda's argument x.  The disambiguated name must be
-- visible at the inner lambda scope.
SELECT 'nested lambdas: PREWHERE';
SELECT * FROM t PREWHERE arrayMap(x -> arrayMap(y -> t.x, [1])[1], [1])[1] > 50;

SELECT 'nested lambdas: WHERE';
SELECT * FROM t WHERE arrayMap(x -> arrayMap(y -> t.x, [1])[1], [1])[1] > 50;

-- Nested lambdas where BOTH have an argument named x and the outer lambda also
-- references its own x.  This forces the inner lambda capture to happen at the
-- outer lambda scope (level 1) instead of the root scope (level 0).  The
-- disambiguated name __table1.x at the outer scope must NOT alias to the outer
-- lambda's argument; it must be a direct INPUT captured from the root scope.
SELECT 'nested lambdas shadowed: PREWHERE';
SELECT * FROM t PREWHERE arrayMap((x, z) -> x + arrayMap(x -> z + t.x, [1])[1], [10], [2])[1] > 100;

SELECT 'nested lambdas shadowed: WHERE';
SELECT * FROM t WHERE arrayMap((x, z) -> x + arrayMap(x -> z + t.x, [1])[1], [10], [2])[1] > 100;

DROP TABLE t;

-- Multi-column table where the lambda body references both columns
-- and one column name collides with the lambda argument.
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t2 VALUES (100, 1)(200, 2);

SELECT 'multi-column: PREWHERE';
SELECT * FROM t2 PREWHERE arrayMap(x -> t2.x + t2.y, [1])[1] > 50;

SELECT 'multi-column: WHERE';
SELECT * FROM t2 WHERE arrayMap(x -> t2.x + t2.y, [1])[1] > 50;

DROP TABLE t2;

-- The colliding name must resolve to the lambda argument's own type, not the column's.
-- Above, column and argument are both UInt32, so a mistyped argument node is invisible;
-- below the types differ and the mismatch surfaces at execution.
DROP TABLE IF EXISTS t3;
CREATE TABLE t3 (v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t3 VALUES (10)(20);

SELECT 'nullable argument, column read first: PREWHERE';
SELECT v FROM t3 PREWHERE arrayExists(v -> (t3.v != 0) AND (v = 7), [toNullable(toUInt64(7))]) ORDER BY v;

SELECT 'nullable argument, column read first: WHERE';
SELECT v FROM t3 WHERE arrayExists(v -> (t3.v != 0) AND (v = 7), [toNullable(toUInt64(7))]) ORDER BY v;

-- Both operand orders must agree: the argument's type cannot depend on which is visited first.
SELECT 'nullable argument, argument read first: PREWHERE';
SELECT v FROM t3 PREWHERE arrayExists(v -> (v = 7) AND (t3.v != 0), [toNullable(toUInt64(7))]) ORDER BY v;

-- tuple() has no short-circuit evaluation, so this pins the order dependence to analysis.
SELECT 'nullable argument, tuple body: PREWHERE';
SELECT v FROM t3 PREWHERE arrayExists(v -> tuple(t3.v != 0, v = 7).2, [toNullable(toUInt64(7))]) ORDER BY v;

SELECT 'nullable argument, nested lambda: PREWHERE';
SELECT v FROM t3 PREWHERE arrayExists(y -> arrayExists(v -> (t3.v != 0) AND (v = 7), [toNullable(toUInt64(7))]), [1]) ORDER BY v;

-- A nested lambda planned beside the collision must not change how the outer binding resolves.
SELECT 'nullable argument, outer binder with a nested lambda: PREWHERE';
SELECT v FROM t3 PREWHERE arrayExists(v -> (t3.v != 0) AND (v = 7) AND arrayExists(x -> plus(x, 2) > 0, [toUInt8(0)]), [toNullable(toUInt64(7))]) ORDER BY v;

-- An unread argument must not be captured: a retained INPUT would change the capture arity.
SELECT 'unread first argument: PREWHERE';
SELECT v FROM t3 PREWHERE arrayExists((a, b) -> b = 7, [1], [toNullable(toUInt64(7))]) ORDER BY v;

-- The body must still read the table column, not the argument that shadows it.
SELECT 'colliding name resolves per site';
SELECT arrayMap(v -> assumeNotNull(v) * 100 + t3.v, [toNullable(toUInt64(3))]) FROM t3 ORDER BY v;

DROP TABLE t3;

-- Nullable column with a non-nullable argument: the mismatch direction follows the types,
-- so the argument takes the wrong type rather than merely losing nullability.
DROP TABLE IF EXISTS t4;
CREATE TABLE t4 (v Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t4 VALUES (10)(20);

SELECT 'nullable column, plain argument: PREWHERE';
SELECT v FROM t4 PREWHERE arrayExists(v -> (t4.v != 0) AND (v = 7), [toUInt64(7)]) ORDER BY v;

DROP TABLE t4;

DROP TABLE IF EXISTS t6;
CREATE TABLE t6 (s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t6 VALUES ('a')('b');

SELECT 'LowCardinality(String) column: PREWHERE';
SELECT s FROM t6 PREWHERE arrayExists(s -> (t6.s != '') AND (s = 'q'), [toNullable('q')]) ORDER BY s;

DROP TABLE t6;

-- An argument name spelled like a generated action name must not displace the body's own node.
SELECT 'argument named after a generated action name';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1), [toUInt8(2)], [toUInt16(42)]);

SELECT 'argument named after a generated action name, nested use';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> materialize(plus(x, 1)) + 10, [toUInt8(2)], [toUInt16(42)]);

SELECT 'argument named after a generated action name, analyzer disabled';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1), [toUInt8(2)], [toUInt16(42)]) SETTINGS enable_analyzer = 0;

-- A nested lambda binds its own argument, so an equal name in the outer lambda is a different
-- binding and must not displace the outer body's own node.
SELECT 'nested lambda argument named after a generated action name';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1) + arrayMap(`plus(x, 1_UInt8)` -> `plus(x, 1_UInt8)`, [toUInt8(0)])[1], [toUInt8(2)], [toUInt16(42)]);

SELECT 'nested lambda argument named after a generated action name, analyzer disabled';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1) + arrayMap(`plus(x, 1_UInt8)` -> `plus(x, 1_UInt8)`, [toUInt8(0)])[1], [toUInt8(2)], [toUInt16(42)]) SETTINGS enable_analyzer = 0;

-- Reading such an argument leaves it and the body's own node claiming one name, so one of the two
-- is lost. Which one is pre-existing analyzer behaviour; the legacy interpreter returns 45.
SELECT 'generated action name argument, read in the body';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1) + `plus(x, 1_UInt8)`, [toUInt8(2)], [toUInt16(42)]);

SELECT 'generated action name argument, read in the body, analyzer disabled';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1) + `plus(x, 1_UInt8)`, [toUInt8(2)], [toUInt16(42)]) SETTINGS enable_analyzer = 0;

-- A table column is named by its qualified identifier here, so it never contends with an
-- argument of the same unqualified name.
DROP TABLE IF EXISTS t7;
CREATE TABLE t7 (`plus(x, 1_UInt8)` UInt16) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t7 VALUES (42);

SELECT 'column named after a generated action name';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1) + t7.`plus(x, 1_UInt8)` * 0, [toUInt8(2)], [toUInt8(9)]) FROM t7;

SELECT 'column named after a generated action name, analyzer disabled';
SELECT arrayMap((x, `plus(x, 1_UInt8)`) -> plus(x, 1) + t7.`plus(x, 1_UInt8)` * 0, [toUInt8(2)], [toUInt8(9)]) FROM t7 SETTINGS enable_analyzer = 0;

DROP TABLE t7;
