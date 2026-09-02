-- Tags: no-fasttest
-- no-fasttest: histogram statistics use the DataSketches KLL library

SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 2;
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;

DROP TABLE IF EXISTS histogram_stats;

CREATE TABLE histogram_stats
(
    id UInt64,
    x Float64 STATISTICS(histogram(8)),
    n Nullable(Int32) STATISTICS(histogram(4))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = '';

SELECT 'Initial definition';
SHOW CREATE TABLE histogram_stats;

INSERT INTO histogram_stats
SELECT
    number,
    if(number < 4500, toFloat64(number % 100), toFloat64(number + 1000000)),
    if(number % 10 = 0, NULL, toInt32(number % 20))
FROM numbers(5000);

INSERT INTO histogram_stats
SELECT
    number + 5000,
    if(number + 5000 < 9000, toFloat64(number % 100), toFloat64(number + 1005000)),
    if((number + 5000) % 10 = 0, NULL, toInt32((number + 5000) % 20))
FROM numbers(5000);

SELECT column, arraySort(groupUniqArray(statistics)) AS part_statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'histogram_stats' AND active
GROUP BY column
ORDER BY column;

SELECT 'Query-scope estimate';
SELECT replaceRegexpAll(explain, '__table1\\.', '')
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM histogram_stats
    WHERE n >= 10 AND n <= 15 AND x > 1000000
)
WHERE explain LIKE '%Prewhere filter column%';

SELECT 'Range predicates';
SELECT count() FROM histogram_stats WHERE x < 50;
SELECT count() FROM histogram_stats WHERE x > 1000000;
SELECT count() FROM histogram_stats WHERE n >= 10 AND n <= 15;

ALTER TABLE histogram_stats DROP STATISTICS x;
ALTER TABLE histogram_stats ADD STATISTICS x TYPE histogram(16);
ALTER TABLE histogram_stats MATERIALIZE STATISTICS x;

SELECT 'Definition after rematerialization';
SHOW CREATE TABLE histogram_stats;

OPTIMIZE TABLE histogram_stats FINAL;

SELECT column, arraySort(groupUniqArray(statistics)) AS part_statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'histogram_stats' AND active
GROUP BY column
ORDER BY column;

SELECT count() FROM histogram_stats WHERE x >= 25 AND x < 75;

DROP TABLE histogram_stats;
