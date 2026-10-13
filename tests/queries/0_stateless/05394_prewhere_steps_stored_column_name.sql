-- Random settings limits: enable_multiple_prewhere_read_steps=(1, 1)
-- A stored column named like a condition of a PREWHERE split into steps returns its stored values.

CREATE TABLE t_mt (x UInt8, k UInt64, `greater(x, 1_UInt8)` UInt8, `greater(k, 5_UInt8)` String, `plus(x, 1_UInt8)` UInt16)
    ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_mem (x UInt8, k UInt64, `greater(x, 1_UInt8)` UInt8, `greater(k, 5_UInt8)` String, `plus(x, 1_UInt8)` UInt16)
    ENGINE = Memory;
INSERT INTO t_mt VALUES (2, 10, 7, 'ten', 70), (0, 20, 8, 'twenty', 80), (3, 30, 9, 'thirty', 90);
INSERT INTO t_mem SELECT * FROM t_mt;

SELECT x, `greater(x, 1_UInt8)` FROM t_mt PREWHERE x > 1 AND k > 5 ORDER BY x;
SELECT x, `greater(x, 1_UInt8)` FROM t_mem PREWHERE x > 1 AND k > 5 ORDER BY x;

-- A String column of that name.
SELECT x, `greater(k, 5_UInt8)` FROM t_mt PREWHERE k > 5 AND x > 1 ORDER BY x;
SELECT x, `greater(k, 5_UInt8)` FROM t_mem PREWHERE k > 5 AND x > 1 ORDER BY x;

-- An expression shared by two steps.
SELECT x, `plus(x, 1_UInt8)` FROM t_mt PREWHERE x + 1 > 2 AND (x + 1) * k > 5 ORDER BY x;

DROP TABLE t_mt;
DROP TABLE t_mem;
