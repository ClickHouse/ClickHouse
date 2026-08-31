-- Partition pruning must not drop rows because of a NaN inside a tuple constant:
-- `(1, 1) < (nan, 1)` is false, so the row is returned.

DROP TABLE IF EXISTS t;

CREATE TABLE t (k Tuple(Int32, Int32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k;
INSERT INTO t VALUES ((1, 1));

SELECT k FROM t WHERE NOT (k < (nan, 1.));
