
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

-- A lambda argument with the name of a table column that the lambda body reads before the argument
-- must keep its own type in the lambda body, also when the query plan is shipped to a remote server.
DROP TABLE IF EXISTS t_prewhere_lambda_shadow;
CREATE TABLE t_prewhere_lambda_shadow (k UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_prewhere_lambda_shadow SELECT number, [number % 5, 1] FROM numbers(10);

SELECT 'shadowed argument read after column: PREWHERE';
SELECT k FROM t_prewhere_lambda_shadow AS tt PREWHERE arrayExists(k -> tt.k = k, arr) ORDER BY k;

SELECT 'shadowed argument read after column: serialized plan';
SELECT k FROM remote('127.0.0.1', currentDatabase(), t_prewhere_lambda_shadow) AS tt PREWHERE arrayExists(k -> tt.k = k, arr) ORDER BY k SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

SELECT 'shadowed argument read before column: serialized plan';
SELECT k FROM remote('127.0.0.1', currentDatabase(), t_prewhere_lambda_shadow) AS tt PREWHERE arrayExists(k -> k = tt.k, arr) ORDER BY k SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

DROP TABLE t_prewhere_lambda_shadow;

DROP TABLE IF EXISTS t_prewhere_lambda_shadow_nullable;
CREATE TABLE t_prewhere_lambda_shadow_nullable (n Nullable(UInt32), arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_lambda_shadow_nullable VALUES (NULL, [1, 2]), (5, [1]), (NULL, [3]);

SELECT 'shadowed nullable column: PREWHERE';
SELECT arr FROM t_prewhere_lambda_shadow_nullable AS tt PREWHERE arrayExists(n -> isNull(tt.n) AND n = 1, arr) ORDER BY arr;

SELECT 'shadowed nullable column: serialized plan';
SELECT arr FROM remote('127.0.0.1', currentDatabase(), t_prewhere_lambda_shadow_nullable) AS tt PREWHERE arrayExists(n -> isNull(tt.n) AND n = 1, arr) ORDER BY arr SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

DROP TABLE t_prewhere_lambda_shadow_nullable;
