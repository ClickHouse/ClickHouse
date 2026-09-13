-- A constant argument of `and`, `or` and `xor` must be converted to a boolean by the same rule that
-- is applied to a non-constant one: every non-zero value is true, including values outside {0, 1}.
-- https://github.com/ClickHouse/ClickHouse/issues/71904

SELECT 1 OR 2.0, 1 OR materialize(2.0);
SELECT 0 OR 2.0, 0 OR materialize(2.0);
SELECT 0 OR -1.5, 0 OR materialize(-1.5);
SELECT 0 OR 0.5, 0 OR materialize(0.5);
SELECT 0 OR 0.0, 0 OR materialize(0.0);
SELECT 0 OR -1.6562789254423896e37, 0 OR materialize(-1.6562789254423896e37);
SELECT 0 OR toFloat32(2.0), 0 OR materialize(toFloat32(2.0));
SELECT 0 OR nan, 0 OR materialize(nan);
SELECT 0 OR inf, 0 OR materialize(inf);
SELECT 0 OR -inf, 0 OR materialize(-inf);

SELECT 1 AND 2.0, 1 AND materialize(2.0);
SELECT 1 AND -1.5, 1 AND materialize(-1.5);
SELECT 1 AND 0.0, 1 AND materialize(0.0);
SELECT 0 AND -1.5, 0 AND materialize(-1.5);

SELECT xor(1, 2.0), xor(1, materialize(2.0));
SELECT xor(0, 2.0), xor(0, materialize(2.0));

-- The three-valued logic path is used when an argument is `Nullable`.
SELECT toNullable(1) OR 2.0, toNullable(1) OR materialize(2.0);
SELECT toNullable(0) AND -1.5, toNullable(0) AND materialize(-1.5);
SELECT toNullable(NULL) OR 2.0, toNullable(NULL) OR materialize(2.0);
SELECT toNullable(NULL) AND 0.0, toNullable(NULL) AND materialize(0.0);

-- The query from the issue: `c0 IS NOT NULL` folds to a constant, which used to make the whole
-- `WHERE` fail while the same expression over a materialized column worked.
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 UInt128, c1 Float32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t0 VALUES (27881038489504535330039081807264208060, -9.16137335578365e+37);
SELECT c1 FROM t0 WHERE (c0 IS NOT NULL) OR (CASE WHEN -4.4216845202190844e+36 = 6.362996997235361e+37 THEN -4.97049591969495e+37 ELSE -1.6562789254423896e+37 END) ORDER BY c1;
DROP TABLE t0;
