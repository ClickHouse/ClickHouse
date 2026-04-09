-- Regression test: filter push-down through aggregation must preserve filter column constness.
-- When an AND expression short-circuits to a constant (e.g., and(LowCardinality(0), 1, aggregate))
-- and parts of it are pushed below aggregation, the remaining expression above must keep the
-- same constness to avoid "Block structure mismatch" or "non constant in source but must be constant" errors.

DROP TABLE IF EXISTS t_const_having;
CREATE TABLE t_const_having (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_const_having VALUES (1), (2), (3);

-- Minimal reproduction: toLowCardinality constant makes AND short-circuit to Const(0),
-- but after push-down the remaining expression was non-const, causing header mismatch.
SELECT
    -c0 AS g,
    toLowCardinality(toUInt8(0)) AND 1 AND MAX(c0) AS h
FROM t_const_having
GROUP BY GROUPING SETS ((g))
HAVING h;

-- Original fuzzer query (simplified)
SELECT
    -c0 AS g,
    greater(isNull(6), toUInt16(toLowCardinality(toUInt256(6))))
        AND 1
        AND sqrt(MAX(c0) OR MAX(c0)) AS h,
    h IS NULL
FROM t_const_having
GROUP BY GROUPING SETS ((g))
HAVING h
ORDER BY g DESC;

DROP TABLE t_const_having;
