-- Old analyzer performs type checks and explain queries throw errors
SET enable_analyzer=1;

EXPLAIN SYNTAX SELECT (-(2)).1;
EXPLAIN SYNTAX SELECT (-'a').1;
EXPLAIN SYNTAX SELECT 1 FROM foo(bar[NOT 2 > 1]);
EXPLAIN SYNTAX SELECT (-(-foo))[bar];

-- Fuzzed
SELECT aggThrow(`t0d0`.`c1`), (-'你们').5
FROM d1.`t26` AS t0d0
    LEFT ARRAY JOIN `t0d0`.`c0.size0` AS `a0`,
    `t0d0`.`c0.null` AS `a1`
    ARRAY JOIN `t0d0`.`c0`[4] AS `a2`
QUALIFY addSeconds(now(), 56)::Time IS NULL
ORDER BY emptyArrayUInt32() ASC NULLS LAST
INTO OUTFILE '/var/lib/clickhouse/user_files/file.data' TRUNCATE
FORMAT Null; -- { serverError UNKNOWN_DATABASE }

SELECT 0.13.5, `t3d0`.`c3`
FROM generate_series(35::Int64, 306601) AS t0d0
    PASTE JOIN deltaLakeCluster(IPv4NumToString(`t0d0`.`generate_series`), '-501:48:29'::Time, (`t0d0`.`generate_series`[(NOT `t0d0`.`generate_series` > ALL(SELECT toRelativeYearNum(`t0d0`.`generate_series`).5 LIMIT 5))] AS `a0`)) AS t1d0
    RIGHT JOIN d0.`t17` AS t3d0 ON `t1d0`.`c1` = `t3d0`.`c0` AND equals(`t1d0`.`c1`, `t3d0`.`c2.size0`[313594649253062377481]) AND (NOT `t0d0`.`generate_series` = `t3d0`.`c2.values`)
    ANTI RIGHT JOIN d0.`t0` AS t4d0 ON or(`t3d0`.`c2.size0` = `t4d0`.`c0`, `a0` = `t4d0`.`c0`)
GROUP BY ROLLUP(`t3d0`.`c3`, 1, `a0`, randBinomial(26::Int64, max(`t3d0`.`c2.values`)), `t4d0`.`c0`.`Float64`) WITH TOTALS
ORDER BY 2 DESC NULLS FIRST, randBinomial(26::Int64, max(`t3d0`.`c2.values`)) ASC NULLS FIRST, `a0` ASC NULLS LAST
INTO OUTFILE '/var/lib/clickhouse/user_files/file.data' TRUNCATE COMPRESSION 'snappy' FORMAT Null; -- { serverError UNKNOWN_DATABASE }

SELECT '2066-01-26'::Date, (-(-`t0d0`.`c1.null`))[`t0d0`.`c1.null`]
FROM d0.`t2` AS t0d0
    FULL JOIN d0.`t2` AS t1d0 ON (NOT `t1d0`.`c1.null` = `t0d0`.`c1`)
PREWHERE `t0d0`.`c1.null` = `t1d0`.`c1`.`Tuple(Decimal)`
WHERE equals(`t1d0`.`c1.null`, addSeconds(now(), 68)::Time)
GROUP BY ROLLUP(`t0d0`.`c1.null`, `t0d0`.`_table`)
HAVING 37::Int16 < `t0d0`.`_table`; -- { serverError UNKNOWN_DATABASE }
