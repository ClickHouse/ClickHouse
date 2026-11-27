-- Tags: no-parallel
-- Reason for no-parallel: this test creates a SQL UDF (`compose`); concurrent runs would
-- race on `CREATE FUNCTION` and `DROP FUNCTION` for this global name.

-- The function composition operator `f | g` is represented by an internal function name,
-- so the name `compose` is not taken and remains available for user defined functions.

SET enable_analyzer = 1;

DROP FUNCTION IF EXISTS compose;
CREATE FUNCTION compose AS (x, y) -> x + y;

SELECT compose(1, 2);
SELECT arrayMap(compose(_1, 1), [1, 2]);
SELECT arrayMap(compose, [1, 2], [3, 4]);
SELECT arrayMap(compose(_, 1) | multiply(_, 2), [1, 2]);

DROP FUNCTION compose;
