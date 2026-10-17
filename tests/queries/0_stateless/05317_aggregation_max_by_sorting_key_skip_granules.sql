-- `optimize_aggregation_max_by_sorting_key`: for `GROUP BY <sorting key prefix>` with only `max`, `argMax`, `min`
-- and `argMin` of the next sorting key column, granules that cannot contain any group's answer are skipped.
-- Every table has 4 rows per granule, and every query is run with the optimization off and on: the results must
-- be equal. `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k String, k2 UInt8, m UInt32, x UInt32) ENGINE = MergeTree ORDER BY (k, k2, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
-- 4 groups (k, k2) of 12 rows each: 3 granules per group, 12 granules.
INSERT INTO t SELECT if(number < 24, 'A', 'B'), intDiv(number, 12) % 2, number % 12 + 1, 1000 - number FROM numbers(48);

SELECT '-- max and argMax: the last granule of each group';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- GROUP BY columns in another order';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k2, k, max(m) FROM t GROUP BY k2, k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k2, k, max(m) FROM t GROUP BY k2, k ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k2, k, max(m) FROM t GROUP BY k2, k ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- min and argMin: the first and the last granule of each group';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, min(m), argMin(x, m) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, min(m), argMin(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, min(m), argMin(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- min and max together';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, min(m), max(m), argMin(x, m), argMax(x, m) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, min(m), max(m), argMin(x, m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, min(m), max(m), argMin(x, m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- a shorter prefix: max of the column after it';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(k2) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(k2) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, max(k2) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- GROUP BY keys that are only some of the columns before m: the last granule of each (k, k2) run';
-- The runs of a group tie on max(m) and min(m), so argMax and argMin would pick any of them; they are not used here.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, max(m) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k2, max(m), min(m) FROM t GROUP BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k2, max(m), min(m) FROM t GROUP BY k2 ORDER BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k2, max(m), min(m) FROM t GROUP BY k2 ORDER BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
-- Without GROUP BY, the implicit projection over column statistics answers max(m) without reading granules,
-- so it is disabled to exercise this optimization.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT max(m) FROM t SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_implicit_projections = 0));
SELECT max(m), min(m) FROM t SETTINGS optimize_aggregation_max_by_sorting_key = 0, optimize_use_implicit_projections = 0;
SELECT max(m), min(m) FROM t SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_implicit_projections = 0;

SELECT '-- WHERE fixes a leading column: GROUP BY k2 for one value of k';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k2, max(m), argMax(x, m) FROM t WHERE k = 'B' GROUP BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k2, max(m), argMax(x, m) FROM t WHERE k = 'B' GROUP BY k2 ORDER BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k2, max(m), argMax(x, m) FROM t WHERE k = 'B' GROUP BY k2 ORDER BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- a GROUP BY expression of the columns before m';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT lower(k) AS lk, max(m) FROM t GROUP BY lk SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT lower(k) AS lk, max(m) FROM t GROUP BY lk ORDER BY lk SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT lower(k) AS lk, max(m) FROM t GROUP BY lk ORDER BY lk SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- not applied: a GROUP BY key that is not a function of the columns before m';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT x % 2 AS p, max(m) FROM t GROUP BY p SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t GROUP BY k, x SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- GROUP BY and DISTINCT without aggregates: the last granule of each run';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2 FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2 FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT DISTINCT k FROM t SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT DISTINCT k FROM t ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT DISTINCT k2 FROM t WHERE k = 'B' SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT DISTINCT k2 FROM t WHERE k = 'B' ORDER BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- aggregates that depend only on the distinct values of the columns before m';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, uniqExact(k2), count(DISTINCT k2), min(k2), sum(DISTINCT k2), groupBitOr(k2) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, uniqExact(k2), count(DISTINCT k2), min(k2), sum(DISTINCT k2), groupBitOr(k2) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, uniqExact(k2), count(DISTINCT k2), min(k2), sum(DISTINCT k2), groupBitOr(k2) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, uniqExact(k2), max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, uniqExact(k2), max(m) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
-- count(DISTINCT) without GROUP BY; the implicit projection over column statistics does not apply to it.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT count(DISTINCT k) FROM t SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_implicit_projections = 0));
SELECT count(DISTINCT k) FROM t SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_implicit_projections = 0;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, groupBitXor(k2) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT DISTINCT x FROM t SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- not applied: max of a column outside the sorting key';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(x) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- not applied: an aggregate that needs every row';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m), count() FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- not applied: a filter on a column outside the primary key';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m) FROM t WHERE x > 980 GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m) FROM t WHERE x > 980 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m) FROM t WHERE m < 10 OR x > 990 GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m) FROM t WHERE m < 10 OR x > 990 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- not applied: a filter the primary key analysis cannot evaluate exactly';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m) FROM t WHERE m IN (3, 7) GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- filters on GROUP BY columns only remove whole groups';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m) FROM t WHERE k = 'A' GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m) FROM t WHERE k = 'A' GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m) FROM t WHERE lower(k) = 'b' AND k2 IN (0, 1) GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m) FROM t WHERE lower(k) = 'b' AND k2 IN (0, 1) GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- filters on m: a granule is skipped only if the first row of the next granule passes the filter';
-- m < 10: the next granule of the one with m 5-8 starts with m = 9, which passes: only the last granule is read.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m), argMax(x, m) FROM t WHERE m < 10 GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m), argMax(x, m) FROM t WHERE m < 10 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, max(m), argMax(x, m) FROM t WHERE m < 10 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
-- m < 9: m = 9 fails, so the granule with m 5-8, which holds the answer, is read.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m), argMax(x, m) FROM t WHERE m < 9 GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m), argMax(x, m) FROM t WHERE m < 9 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, max(m), argMax(x, m) FROM t WHERE m < 9 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
-- min: the first row of the previous granule must pass the filter.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, min(m), max(m) FROM t WHERE m BETWEEN 3 AND 10 GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, min(m), max(m) FROM t WHERE m BETWEEN 3 AND 10 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, min(m), max(m) FROM t WHERE m BETWEEN 3 AND 10 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
-- A conjunct on GROUP BY columns and one on m, as PREWHERE.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m) FROM t PREWHERE k = 'B' AND m <= 10 GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, max(m) FROM t PREWHERE k = 'B' AND m <= 10 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, max(m) FROM t PREWHERE k = 'B' AND m <= 10 GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- ORDER BY ... LIMIT n BY: the last n rows (DESC) or the first n rows (ASC) of each run';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, m, x FROM t ORDER BY k, k2, m DESC LIMIT 1 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, m, x FROM t ORDER BY k, k2, m DESC LIMIT 1 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
-- The last granule of a run may hold rows of the next run, so it proves only one row of its run: LIMIT 2 also keeps the
-- granule before it.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, m, x FROM t ORDER BY k, k2, m DESC LIMIT 1 OFFSET 1 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, m, x FROM t ORDER BY k, k2, m DESC LIMIT 1 OFFSET 1 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, m, x FROM t ORDER BY k, k2, m DESC LIMIT 1 OFFSET 1 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, m, x FROM t ORDER BY k, k2, m LIMIT 3 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, k2, m, x FROM t ORDER BY k, k2, m LIMIT 3 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, m, x FROM t ORDER BY k, k2, m LIMIT 3 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k2, m, x FROM t WHERE k = 'B' AND m < 10 ORDER BY k2, m DESC LIMIT 1 BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k2, m, x FROM t WHERE k = 'B' AND m < 10 ORDER BY k2, m DESC LIMIT 1 BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k2, m, x FROM t WHERE k = 'B' AND m < 10 ORDER BY k2, m DESC LIMIT 1 BY k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1;
-- Not applied: a tie-breaker after m could prefer a row of a skipped granule.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, m, x FROM t ORDER BY k, k2, m DESC, x LIMIT 1 BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, x FROM t ORDER BY k, x DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- parts of a projection that was chosen: the projection sorting key is used';
DROP TABLE IF EXISTS t_projection;
CREATE TABLE t_projection (symbol String, ts UInt32, price UInt32, PROJECTION by_symbol (SELECT * ORDER BY symbol, ts))
    ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_projection SELECT ['A', 'B', 'C', 'D'][number % 4 + 1], number, 1000 - number FROM numbers(48);
-- The projection is chosen because its primary key narrows the read to B and C.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT symbol, max(ts), argMax(price, ts) FROM t_projection WHERE symbol IN ('B', 'C') GROUP BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_projections = 1)
WHERE explain LIKE '%ReadFromMergeTree%';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT symbol, max(ts), argMax(price, ts) FROM t_projection WHERE symbol IN ('B', 'C') GROUP BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_projections = 1));
SELECT symbol, max(ts), argMax(price, ts) FROM t_projection WHERE symbol IN ('B', 'C') GROUP BY symbol ORDER BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 0, optimize_use_projections = 1;
SELECT symbol, max(ts), argMax(price, ts) FROM t_projection WHERE symbol IN ('B', 'C') GROUP BY symbol ORDER BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_projections = 1;
-- The projection is chosen for its sort order.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT symbol, ts, price FROM t_projection ORDER BY symbol, ts DESC LIMIT 1 BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_projections = 1, optimize_read_in_order = 1));
SELECT symbol, ts, price FROM t_projection ORDER BY symbol, ts DESC LIMIT 1 BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 0, optimize_use_projections = 1, optimize_read_in_order = 1;
SELECT symbol, ts, price FROM t_projection ORDER BY symbol, ts DESC LIMIT 1 BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_projections = 1, optimize_read_in_order = 1;
-- Without a filter the base table is read; it is sorted by ts, so the rule does not apply.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT symbol, max(ts) FROM t_projection GROUP BY symbol SETTINGS optimize_aggregation_max_by_sorting_key = 1, optimize_use_projections = 1));

SELECT '-- every part is filtered on its own';
DROP TABLE IF EXISTS t_parts;
CREATE TABLE t_parts (k String, m UInt32) ENGINE = MergeTree ORDER BY (k, m) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_parts;
INSERT INTO t_parts SELECT 'A', number + 1 FROM numbers(12);
INSERT INTO t_parts SELECT 'A', number + 101 FROM numbers(12);
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m), min(m) FROM t_parts GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m), min(m) FROM t_parts GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, max(m), min(m) FROM t_parts GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- gaps shorter than merge_tree_min_rows_for_seek or merge_tree_min_bytes_for_seek are read through';
-- max keeps granules 2, 5, 8 and 11: gaps of 2 granules (8 rows). min also keeps 0, 3, 6 and 9: gaps of 1 granule.
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_rows_for_seek = 4));
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_rows_for_seek = 4;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_rows_for_seek = 8));
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_rows_for_seek = 8;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, min(m), argMin(x, m) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_rows_for_seek = 4));
SELECT k, k2, min(m), argMin(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, min(m), argMin(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_rows_for_seek = 4;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_bytes_for_seek = 1000000));
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, k2, max(m), argMax(x, m) FROM t GROUP BY k, k2 ORDER BY k, k2 SETTINGS optimize_aggregation_max_by_sorting_key = 1, merge_tree_min_bytes_for_seek = 1000000;

SELECT '-- not applied: reverse sorting key';
DROP TABLE IF EXISTS t_reverse;
CREATE TABLE t_reverse (k String, m UInt32) ENGINE = MergeTree ORDER BY (k, m DESC)
    SETTINGS allow_experimental_reverse_key = 1, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_reverse SELECT 'A', number + 1 FROM numbers(12);
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_reverse GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t_reverse GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- not applied: Nullable column (max ignores the NULLs at the end of the group)';
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (k String, m Nullable(UInt32)) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS allow_nullable_key = 1, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_nullable SELECT 'A', if(number < 6, number + 1, NULL) FROM numbers(12);
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_nullable GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t_nullable GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- not applied: floating-point column (NaN at the end of the group)';
DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float (k String, m Float64) ENGINE = MergeTree ORDER BY (k, m) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_float SELECT 'A', if(number < 6, number + 1, nan) FROM numbers(12);
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_float GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t_float GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- not applied: floating-point prefix (-0 and +0 sort as equal but may be different groups)';
-- Only the plan is checked: whether -0 and +0 form one group is decided by the grouping, not by this optimization
-- (it differs between hash grouping and aggregation in order, see 04312_in_order_aggregation_inconsistency).
DROP TABLE IF EXISTS t_float_prefix;
CREATE TABLE t_float_prefix (k Float64, m UInt32) ENGINE = MergeTree ORDER BY (k, m) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_float_prefix SELECT if(number < 6, -0., 0.), number FROM numbers(12);
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_float_prefix GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- not applied: an expression in the sorting key';
DROP TABLE IF EXISTS t_expression;
CREATE TABLE t_expression (k String, m UInt32) ENGINE = MergeTree ORDER BY (k, intDiv(m, 5)) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_expression SELECT 'A', number FROM numbers(12);
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_expression GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- not applied: FINAL';
DROP TABLE IF EXISTS t_final;
CREATE TABLE t_final (k String, m UInt32) ENGINE = ReplacingMergeTree ORDER BY (k, m) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_final SELECT 'A', number + 1 FROM numbers(12);
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_final FINAL GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- part read in full: lightweight DELETE';
DROP TABLE IF EXISTS t_deleted;
CREATE TABLE t_deleted (k String, m UInt32) ENGINE = MergeTree ORDER BY (k, m) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_deleted SELECT 'A', number + 1 FROM numbers(12);
DELETE FROM t_deleted WHERE m >= 9;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_deleted GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t_deleted GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- part read in full: DELETE written as a patch part';
DROP TABLE IF EXISTS t_patched;
CREATE TABLE t_patched (k String, m UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patched SELECT 'A', number + 1 FROM numbers(12);
DELETE FROM t_patched WHERE m >= 9 SETTINGS lightweight_delete_mode = 'lightweight_update';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_patched GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t_patched GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- part read in full: ALTER DELETE applied on the fly';
DROP TABLE IF EXISTS t_on_fly;
CREATE TABLE t_on_fly (k String, m UInt32) ENGINE = MergeTree ORDER BY (k, m) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_on_fly;
INSERT INTO t_on_fly SELECT 'A', number + 1 FROM numbers(12);
ALTER TABLE t_on_fly DELETE WHERE m >= 9 SETTINGS mutations_sync = 0;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t_on_fly GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1, apply_mutations_on_fly = 1));
SELECT k, max(m) FROM t_on_fly GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1, apply_mutations_on_fly = 1;

DROP TABLE t;
DROP TABLE t_parts;
DROP TABLE t_reverse;
DROP TABLE t_nullable;
DROP TABLE t_float;
DROP TABLE t_float_prefix;
DROP TABLE t_expression;
DROP TABLE t_final;
DROP TABLE t_deleted;
DROP TABLE t_patched;
DROP TABLE t_on_fly;
DROP TABLE t_projection;
