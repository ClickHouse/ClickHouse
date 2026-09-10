-- Partition pruning must not drop rows because of a NaN inside a tuple constant:
-- `(1, 1) < (nan, 1)` is false, so the row is returned.

DROP TABLE IF EXISTS t;

CREATE TABLE t (k Tuple(Int32, Int32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k;
INSERT INTO t VALUES ((1, 1));

SELECT k FROM t WHERE NOT (k < (nan, 1.));

-- The constant can also hold a NaN only after it is converted to the key type:
-- `'nan'` becomes a Float64 NaN, and `1 < nan` is false, so the row is returned.

DROP TABLE IF EXISTS t2;

CREATE TABLE t2 (f Float64) ENGINE = MergeTree ORDER BY f;
INSERT INTO t2 VALUES (1);

SELECT f FROM t2 WHERE NOT (f < 'nan');

-- A constant holding no NaN must still reach one granule: both arms above also hold when every
-- comparison atom is declined, which silently disables pruning.

DROP TABLE IF EXISTS t3;

CREATE TABLE t3 (f Float64) ENGINE = MergeTree ORDER BY f
SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t3 VALUES (1), (2);

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t3 WHERE f = 1)
WHERE explain ILIKE '%Granules: 1/2%';
