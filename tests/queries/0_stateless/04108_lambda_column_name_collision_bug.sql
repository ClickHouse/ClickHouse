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
SELECT 'sibling lambdas: capture + argument';
SELECT * FROM t PREWHERE arrayMap(x -> *, [1])[1] + arrayMap(x -> x + 1, [1])[1] > 50;

SELECT 'sibling lambdas reversed';
SELECT * FROM t PREWHERE arrayMap(x -> x + 1, [1])[1] + arrayMap(x -> *, [1])[1] > 50;

DROP TABLE t;
