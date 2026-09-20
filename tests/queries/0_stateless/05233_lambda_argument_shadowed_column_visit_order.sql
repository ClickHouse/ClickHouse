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

DROP TABLE t_05233;

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
