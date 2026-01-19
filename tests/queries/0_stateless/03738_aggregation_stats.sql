CREATE TABLE t1 (n UInt64) ENGINE = MergeTree() SETTINGS auto_statistics_types='uniq';
CREATE TABLE t2 (key1 UInt64, key2 UInt64, key3 UInt64, value UInt64) ENGINE = MergeTree() SETTINGS auto_statistics_types='uniq';

INSERT INTO t1 SELECT * FROM numbers(5);
INSERT INTO t2 SELECT number%10 AS key1, number%3 AS key2, number%17 AS key3, number AS value FROM numbers(100);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET use_statistics = 1;
SET enable_parallel_replicas = 0;

SELECT '====== Aggregation by 1 column ======';
SELECT explain FROM
(
EXPLAIN keep_logical_steps=1, actions=1
SELECT *
FROM t1
JOIN (SELECT key1 AS key, sum(value) FROM t2 GROUP BY key) AS tt2
ON t1.n = tt2.key
)
WHERE explain LIKE '% Join%' OR explain LIKE '% ResultRows:%' OR explain LIKE '% ReadFromMergeTree%' OR explain LIKE '% Aggregating%';


SELECT '===== Aggregation by 2 columns and NDV1*NDV2 < total rows ======';
SELECT explain FROM
(
EXPLAIN keep_logical_steps=1, actions=1
SELECT *
FROM t1
JOIN (SELECT key1, key2, sum(value) FROM t2 GROUP BY key1, key2) AS tt2
ON t1.n = tt2.key1
)
WHERE explain LIKE '% Join%' OR explain LIKE '% ResultRows:%' OR explain LIKE '% ReadFromMergeTree%' OR explain LIKE '% Aggregating%';


SELECT '===== Aggregation by 2 columns and NDV1*NDV2 > total rows ======';
SELECT explain FROM
(
EXPLAIN keep_logical_steps=1, actions=1
SELECT *
FROM t1
JOIN (SELECT key1, key3, sum(value) FROM t2 GROUP BY key1, key3) AS tt2
ON t1.n = tt2.key1
)
WHERE explain LIKE '% Join%' OR explain LIKE '% ResultRows:%' OR explain LIKE '% ReadFromMergeTree%' OR explain LIKE '% Aggregating%';


SELECT '====== Aggregation by 0 columns =====';
SELECT explain FROM
(
EXPLAIN keep_logical_steps=1, actions=1
SELECT *
FROM t1
JOIN (SELECT 7 AS key, sum(value) FROM t2) AS tt2
ON t1.n = tt2.key
)
WHERE explain LIKE '% Join%' OR explain LIKE '% ResultRows:%' OR explain LIKE '% ReadFromMergeTree%' OR explain LIKE '% Aggregating%';
