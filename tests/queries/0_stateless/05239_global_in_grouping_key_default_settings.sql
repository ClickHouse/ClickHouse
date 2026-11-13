-- Tags: shard

-- A `GLOBAL IN` that is both a GROUP BY key and a SELECT list item must be read back from the
-- aggregated column, so the rows where that key is absent report its default value.

DROP TABLE IF EXISTS t_05239;
SET enable_analyzer = 1;
SET group_by_use_nulls = 0;

DROP TABLE IF EXISTS t_05239;

CREATE TABLE t_05239 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05239 VALUES (1);

SELECT 'GROUPING SETS, IN';
SELECT 1 IN (t_05239) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((1 IN (t_05239)), ())
ORDER BY s;

SELECT 'GROUPING SETS, GLOBAL IN';
SELECT 1 GLOBAL IN (t_05239) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((1 GLOBAL IN (t_05239)), ())
ORDER BY s;

SELECT 'WITH TOTALS, IN';
SELECT 1 IN (t_05239) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY s WITH TOTALS
ORDER BY s;

SELECT 'WITH TOTALS, GLOBAL IN';
SELECT 1 GLOBAL IN (t_05239) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY s WITH TOTALS
ORDER BY s;

DROP TABLE t_05239;
