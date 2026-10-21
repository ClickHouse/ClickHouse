-- Inside a lambda body, a name the lambda declares as an argument must mean that argument, and an outer
-- column of the same name has to reach the body under a different name. The planner creates the lambda
-- argument lazily, on the first reference to the name, so when the outer column came first it created the
-- argument and gave it the outer column's type. The body was then built for a type the argument does not
-- have, and executing it threw `Unexpected return type from plus`.
--
-- Only the operand order differs within each pair of queries below; both orders must give the same answer.

DROP TABLE IF EXISTS t_05233;
CREATE TABLE t_05233 (value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05233 VALUES (10);

-- `PREWHERE` does not use column identifiers as action node names, so the table column keeps the name
-- `value`, which is also the lambda argument. `toUInt8(t_05233.value) + 1` matches the single row.
SELECT value FROM t_05233 PREWHERE arrayMap(value -> toUInt8(t_05233.value) + value, [1])[1] = 11;
SELECT value FROM t_05233 PREWHERE arrayMap(value -> value + toUInt8(t_05233.value), [1])[1] = 11;

-- The same one lambda deeper: the nested lambda references the argument of the outer lambda first,
-- so the table column must not reuse that argument node. `y` is unused; the result is 1 + 10 = 11.
SELECT value FROM t_05233 PREWHERE arrayMap(value -> arrayMap(y -> value + t_05233.value, [1])[1], [toUInt8(1)])[1] = 11;
SELECT value FROM t_05233 PREWHERE arrayMap(value -> arrayMap(y -> t_05233.value + value, [1])[1], [toUInt8(1)])[1] = 11;
SELECT value FROM t_05233 PREWHERE arrayMap(value -> arrayMap(y -> arrayMap(z -> value + t_05233.value, [1])[1], [1])[1], [toUInt8(1)])[1] = 11;

-- A correlated column of the outer query cannot be read together with the table of the subquery, and binding
-- `PREWHERE` to the outer table would filter the outer query instead, so it is rejected in `PREWHERE`,
-- whether captured by a lambda or not.
SET allow_experimental_correlated_subqueries = 1;
SELECT 1 FROM t_05233 AS t1 WHERE EXISTS (SELECT 1 FROM t_05233 AS t2 PREWHERE arrayMap(value -> t1.value + value, [toUInt8(1)])[1] = 11); -- { serverError ILLEGAL_PREWHERE }
SELECT 1 FROM t_05233 AS t1 WHERE EXISTS (SELECT 1 FROM t_05233 AS t2 PREWHERE arrayMap(value -> value + t1.value, [toUInt8(1)])[1] = 11); -- { serverError ILLEGAL_PREWHERE }
SELECT 1 FROM t_05233 AS t1 WHERE EXISTS (SELECT 1 FROM t_05233 AS t2 PREWHERE arrayMap(value -> t2.value + t1.value + value, [toUInt8(1)])[1] = 21); -- { serverError ILLEGAL_PREWHERE }
SELECT 1 FROM t_05233 AS t1 WHERE EXISTS (SELECT 1 FROM t_05233 AS t2 PREWHERE t2.value + arrayMap(value -> t1.value + value, [toUInt8(1)])[1] = 21); -- { serverError ILLEGAL_PREWHERE }
SELECT 1 FROM t_05233 AS t1 WHERE EXISTS (SELECT 1 FROM t_05233 AS t2 PREWHERE t1.value + 1 = 11); -- { serverError ILLEGAL_PREWHERE }
SELECT value, (SELECT count() FROM t_05233 AS t2 PREWHERE arrayMap(x -> t1.value + x, [toUInt8(1)])[1] = 11) FROM t_05233 AS t1; -- { serverError ILLEGAL_PREWHERE }

-- In `WHERE` the shadowed correlated column works. `t1.value + 1` matches the single row.
SELECT 1 FROM t_05233 AS t1 WHERE EXISTS (SELECT 1 FROM t_05233 AS t2 WHERE arrayMap(value -> t1.value + value, [toUInt8(1)])[1] = 11);
SELECT 1 FROM t_05233 AS t1 WHERE EXISTS (SELECT 1 FROM t_05233 AS t2 WHERE arrayMap(value -> value + t1.value, [toUInt8(1)])[1] = 11);

DROP TABLE t_05233;

-- A quoted column name may coincide with the column identifier of another column, here `__table1.value`.
-- It must not share a node with the shadowed column `t.value`, whichever is visited first. 100 + 10 + 1 = 111.
DROP TABLE IF EXISTS t_05233_identifier;
CREATE TABLE t_05233_identifier (`__table1.value` UInt64, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05233_identifier VALUES (100, 10);
SELECT value FROM t_05233_identifier AS t PREWHERE arrayMap(value -> `__table1.value` + t.value + value, [toUInt8(1)])[1] = 111;
SELECT value FROM t_05233_identifier AS t PREWHERE arrayMap(value -> t.value + `__table1.value` + value, [toUInt8(1)])[1] = 111;
DROP TABLE t_05233_identifier;

-- The synthetic column of an `INTERPOLATE` expression has no column identifier at all.
-- The interpolated `value` is 10, so `d` is `10 % 3` = 1 and the fill row is 1 + (1 + 1) = 3.
SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS (moduloOrNull(value, 3) AS d) + arrayMap(value -> d + value, [1])[1]);

SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS (moduloOrNull(value, 3) AS d) + arrayMap(value -> value + d, [1])[1]);

-- The same without `Nullable`: `d` is `UInt16` while the lambda argument is `UInt8`.
SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS (toUInt8(value) + 1 AS d) + arrayMap(value -> d + value, [1])[1]);

SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS (toUInt8(value) + 1 AS d) + arrayMap(value -> value + d, [1])[1]);

-- A matcher inside a lambda in `PREWHERE` takes the `USING` column from the left table, like an identifier does,
-- so the lambda reads only the table of `PREWHERE`. A column of the other table is rejected.
DROP TABLE IF EXISTS t_05233_using_left;
DROP TABLE IF EXISTS t_05233_using_right;
CREATE TABLE t_05233_using_left (a UInt8, v UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_05233_using_right (a UInt8, w UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05233_using_left VALUES (1, 10), (9, 90);
INSERT INTO t_05233_using_right VALUES (1, 11), (5, 55);
SELECT a, v, w FROM t_05233_using_left FULL JOIN t_05233_using_right USING (a) PREWHERE arrayMap(z -> tuple(* EXCEPT w), [1])[1].1 = 9 ORDER BY a SETTINGS join_use_nulls = 1;
SELECT a, v, w FROM t_05233_using_left RIGHT JOIN t_05233_using_right USING (a) PREWHERE arrayMap(z -> tuple(* EXCEPT w), [1])[1].1 = 1 ORDER BY a SETTINGS join_use_nulls = 1;
SELECT a, v, w FROM t_05233_using_left FULL JOIN t_05233_using_right USING (a) PREWHERE arrayMap(z -> tuple(*), [1])[1].1 = 9 ORDER BY a SETTINGS join_use_nulls = 1; -- { serverError ILLEGAL_PREWHERE }
DROP TABLE t_05233_using_left;
DROP TABLE t_05233_using_right;
