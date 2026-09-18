-- Limits over unordered input can consume any distinct values. Global sorting is preserved
-- independently of whether the sorted `DISTINCT` algorithm is enabled.
SET max_threads = 4;
SET max_block_size = 1000;
SET allow_parallel_distinct = 1;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

-- Unordered consumers permit hash partitioning.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) OFFSET 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT -5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 0.1);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 2 AS g, number % 100 AS k FROM numbers_mt(100000) LIMIT 1 BY g);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT count() FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000)));

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000)) LIMIT 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT number % 100 AS k FROM numbers_mt(100000) UNION DISTINCT SELECT number % 100 AS k FROM numbers_mt(100000) SETTINGS limit = 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT number % 100 AS k FROM numbers_mt(100000) UNION DISTINCT SELECT number % 100 AS k FROM numbers_mt(100000)) LIMIT 1 BY k);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(100000) LIMIT AFTER number = 10 UNTIL number = 20);

SET optimize_distinct_in_order = 1;

-- A global sort order remains valid across final deduplication.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM (SELECT number % 100 AS k FROM numbers_mt(100000) ORDER BY k) OFFSET 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 2 AS g, number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 1 BY g);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 5);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 5 OFFSET 2);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) OFFSET 5);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT -5);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 0.1);

SELECT count(), uniqExact(g) FROM (SELECT DISTINCT number % 2 AS g, number % 100 AS k FROM numbers_mt(100000) LIMIT 1 BY g);

SELECT groupArray(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 5 OFFSET 2);

SELECT groupArray(k) FROM (SELECT DISTINCT k FROM (SELECT number % 100 AS k FROM numbers_mt(100000) ORDER BY k) LIMIT -5);

SELECT groupArray(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT AFTER k = 10 UNTIL k = 20);

SELECT arraySort(groupArray((g, k))) FROM (SELECT DISTINCT number % 2 AS g, number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 1 BY g);

SET optimize_distinct_in_order = 0;

-- A global sort order remains valid across final deduplication.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM (SELECT number % 100 AS k FROM numbers_mt(100000) ORDER BY k) OFFSET 5);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 2 AS g, number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 1 BY g);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 5);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 5 OFFSET 2);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) OFFSET 5);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT -5);

SELECT count(), uniqExact(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 0.1);

SELECT count(), uniqExact(g) FROM (SELECT DISTINCT number % 2 AS g, number % 100 AS k FROM numbers_mt(100000) LIMIT 1 BY g);

SELECT groupArray(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 5 OFFSET 2);

SELECT groupArray(k) FROM (SELECT DISTINCT k FROM (SELECT number % 100 AS k FROM numbers_mt(100000) ORDER BY k) LIMIT -5);

SELECT groupArray(k) FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT AFTER k = 10 UNTIL k = 20);

SELECT arraySort(groupArray((g, k))) FROM (SELECT DISTINCT number % 2 AS g, number % 100 AS k FROM numbers_mt(100000) ORDER BY k LIMIT 1 BY g);

-- Limit hints bound an unbounded input without requiring an order on its distinct values.
SELECT count(), uniqExact(number) FROM (SELECT DISTINCT number FROM system.numbers_mt LIMIT 5)
SETTINGS max_execution_time = 10;

-- Partition-disjoint `DISTINCT` streams also feed a single global limit correctly.
SET allow_parallel_distinct = 0;
SET allow_distinct_partitions_independently = 1;
SET force_distinct_partitions_independently = 1;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET optimize_distinct_in_order = 0;
DROP TABLE IF EXISTS distinct_order_requirements;
CREATE TABLE distinct_order_requirements (k UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k % 4;
INSERT INTO distinct_order_requirements SELECT number % 100 FROM numbers(10000);

SELECT countIf(explain LIKE '%Skip stream merging: 1%') > 0
FROM (EXPLAIN actions = 1 SELECT DISTINCT k FROM distinct_order_requirements LIMIT 5);
SELECT count(), uniqExact(k) FROM (SELECT DISTINCT k FROM distinct_order_requirements LIMIT 5 OFFSET 2);
SELECT count(), uniqExact(g) FROM (SELECT DISTINCT k, k % 2 AS g FROM distinct_order_requirements LIMIT 1 BY g);
SELECT groupArray(k) FROM (SELECT DISTINCT k FROM distinct_order_requirements ORDER BY k LIMIT 5);

DROP TABLE distinct_order_requirements;
