-- Tags: no-fasttest
-- no-fasttest: histogram statistics use the DataSketches KLL and frequent items libraries

SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 2;
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;

DROP TABLE IF EXISTS histogram_mcv;
DROP TABLE IF EXISTS histogram_mcv_auto;
DROP TABLE IF EXISTS histogram_mcv_bad;

-- `a` is skewed: 42 holds half of the rows, 4242 a tenth, and the rest is spread over 0..9999.
-- `b` is uniform over 0..999.
CREATE TABLE histogram_mcv
(
    id UInt64,
    a UInt32 STATISTICS(histogram(16, 8)),
    b UInt32 STATISTICS(histogram(16, 8))
)
ENGINE = MergeTree
ORDER BY id
-- Compact parts have no per-column sizes, so the PREWHERE order depends on the estimated selectivity only.
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

SELECT 'Definition';
SHOW CREATE TABLE histogram_mcv;

-- Keep the four parts, so the query-time estimator merges their statistics.
SYSTEM STOP MERGES histogram_mcv;

INSERT INTO histogram_mcv SELECT number, multiIf(number % 10 < 5, 42, number % 10 = 5, 4242, (number * 7919) % 10000), number % 1000 FROM numbers(0, 25000);
INSERT INTO histogram_mcv SELECT number, multiIf(number % 10 < 5, 42, number % 10 = 5, 4242, (number * 7919) % 10000), number % 1000 FROM numbers(25000, 25000);
INSERT INTO histogram_mcv SELECT number, multiIf(number % 10 < 5, 42, number % 10 = 5, 4242, (number * 7919) % 10000), number % 1000 FROM numbers(50000, 25000);
INSERT INTO histogram_mcv SELECT number, multiIf(number % 10 < 5, 42, number % 10 = 5, 4242, (number * 7919) % 10000), number % 1000 FROM numbers(75000, 25000);

SELECT column, arraySort(groupUniqArray(statistics)) AS part_statistics, count() AS parts
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'histogram_mcv' AND active
GROUP BY column
ORDER BY column;

-- The estimates combine the most common values with the residual histogram: the frequent value 42 (50%)
-- goes after `b < 100` (10%), the second most common value 4242 (10%) before `b < 300` (30%), and the
-- point mass of 42 is excluded from `a < 42` but included in `a <= 42`.
SELECT 'Query-scope estimates';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE a = 42 AND b < 100) WHERE explain LIKE '%Prewhere filter column%';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE b < 300 AND a = 4242) WHERE explain LIKE '%Prewhere filter column%';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE b < 100 AND a < 42) WHERE explain LIKE '%Prewhere filter column%';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE a <= 42 AND b < 100) WHERE explain LIKE '%Prewhere filter column%';

SELECT 'Results';
SELECT count() FROM histogram_mcv WHERE a = 42 AND b < 100;
SELECT count() FROM histogram_mcv WHERE b < 300 AND a = 4242;
SELECT count() FROM histogram_mcv WHERE b < 100 AND a < 42;
SELECT count() FROM histogram_mcv WHERE a <= 42 AND b < 100;

SELECT 'After OPTIMIZE FINAL';
SYSTEM START MERGES histogram_mcv;
OPTIMIZE TABLE histogram_mcv FINAL;

SELECT column, arraySort(groupUniqArray(statistics)) AS part_statistics, count() AS parts
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'histogram_mcv' AND active
GROUP BY column
ORDER BY column;

SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE a = 42 AND b < 100) WHERE explain LIKE '%Prewhere filter column%';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE b < 300 AND a = 4242) WHERE explain LIKE '%Prewhere filter column%';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE b < 100 AND a < 42) WHERE explain LIKE '%Prewhere filter column%';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE a <= 42 AND b < 100) WHERE explain LIKE '%Prewhere filter column%';

SELECT 'ALTER';
ALTER TABLE histogram_mcv DROP STATISTICS a;
ALTER TABLE histogram_mcv ADD STATISTICS a TYPE histogram(32, 100);
ALTER TABLE histogram_mcv MATERIALIZE STATISTICS a;
ALTER TABLE histogram_mcv MODIFY STATISTICS b TYPE histogram(8, 4);
ALTER TABLE histogram_mcv MATERIALIZE STATISTICS b;
SHOW CREATE TABLE histogram_mcv;

SELECT column, arraySort(groupUniqArray(statistics)) AS part_statistics, count() AS parts
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'histogram_mcv' AND active
GROUP BY column
ORDER BY column;

SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE a = 42 AND b < 100) WHERE explain LIKE '%Prewhere filter column%';
SELECT replaceRegexpAll(explain, '__table1\\.', '') FROM (EXPLAIN actions = 1 SELECT count() FROM histogram_mcv WHERE b < 300 AND a = 4242) WHERE explain LIKE '%Prewhere filter column%';

SELECT 'auto_statistics_types';
CREATE TABLE histogram_mcv_auto
(
    id UInt64,
    v UInt32,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = 'histogram(8, 16)';

INSERT INTO histogram_mcv_auto SELECT number, if(number % 3 = 0, 7, number), toString(number) FROM numbers(3000);

SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'histogram_mcv_auto' AND active
ORDER BY column;

SELECT count() FROM histogram_mcv_auto WHERE v = 7;

SELECT 'Invalid parameters';
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(8))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(8, 16, 4))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(1, 16))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(1025, 16))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(8, 0))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(8, 769))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(8, 16.5))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(8, -1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE histogram_mcv_bad (x String STATISTICS(histogram(8, 16))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }
CREATE TABLE histogram_mcv_bad (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'histogram(8)'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE histogram_mcv ADD STATISTICS id TYPE histogram(8); -- { serverError BAD_ARGUMENTS }
ALTER TABLE histogram_mcv MODIFY STATISTICS a TYPE histogram(8, 1000); -- { serverError BAD_ARGUMENTS }

CREATE TABLE histogram_mcv_bad (x UInt32 STATISTICS(histogram(1024, 768))) ENGINE = MergeTree ORDER BY tuple();
SHOW CREATE TABLE histogram_mcv_bad;

DROP TABLE histogram_mcv;
DROP TABLE histogram_mcv_auto;
DROP TABLE histogram_mcv_bad;
