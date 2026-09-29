-- `query_plan_window_functions_hash_partitioning` with `allow_window_partitions_independently`: when the table
-- partition key is determined by the window `PARTITION BY` columns, every partition is read through its own
-- stream and the hash scatter is skipped. The results must not change.

-- The cost heuristic depends on `max_threads` and the number of partitions.
SET max_threads = 8, force_window_partitions_independently = 1;
-- The optimization is disabled under parallel replicas and with sort limits.
SET enable_parallel_replicas = 0, max_rows_to_sort = 0, max_bytes_to_sort = 0;
-- Hash partitioning is not used when the storage ordering may be reused for the window sort.
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_hash_window_partitions;
CREATE TABLE t_hash_window_partitions (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k % 8;
SYSTEM STOP MERGES t_hash_window_partitions;
INSERT INTO t_hash_window_partitions SELECT number % 64, number FROM numbers(1000);
INSERT INTO t_hash_window_partitions SELECT number % 64, number + 1000 FROM numbers(1000);

DROP TABLE IF EXISTS t_hash_window_other_partitions;
CREATE TABLE t_hash_window_other_partitions (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY x % 8;
INSERT INTO t_hash_window_other_partitions SELECT * FROM t_hash_window_partitions;

-- { echo }

-- The partition key is a function of the window `PARTITION BY` column: the scatter is skipped.
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT k, sum(x) OVER (PARTITION BY k) FROM t_hash_window_partitions SETTINGS query_plan_window_functions_hash_partitioning = 1)
WHERE explain LIKE '%Hash partitioning%' OR explain LIKE '%Skip scatter by partition%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT k, sum(x) OVER (PARTITION BY k) FROM t_hash_window_partitions SETTINGS query_plan_window_functions_hash_partitioning = 1)
WHERE explain LIKE '%ScatterByPartitionTransform%';
SELECT (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(x) OVER (PARTITION BY k) AS s FROM t_hash_window_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(x) OVER (PARTITION BY k) AS s FROM t_hash_window_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 1);

-- `DISTINCT` after the window.
SELECT (SELECT groupArraySorted(100)((k, s)) FROM (SELECT DISTINCT k, sum(x) OVER (PARTITION BY k) AS s FROM t_hash_window_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT groupArraySorted(100)((k, s)) FROM (SELECT DISTINCT k, sum(x) OVER (PARTITION BY k) AS s FROM t_hash_window_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 1);

-- Another window after the hash window.
SELECT (SELECT sum(cityHash64(k, x, s, m, r)) FROM (SELECT k, x, sum(x) OVER (PARTITION BY k) AS s, max(x) OVER (PARTITION BY k % 3) AS m, row_number() OVER (PARTITION BY k ORDER BY x) AS r FROM t_hash_window_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(k, x, s, m, r)) FROM (SELECT k, x, sum(x) OVER (PARTITION BY k) AS s, max(x) OVER (PARTITION BY k % 3) AS m, row_number() OVER (PARTITION BY k ORDER BY x) AS r FROM t_hash_window_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 1);

-- The partition key is not determined by the window `PARTITION BY` column: the scatter stays.
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT k, sum(x) OVER (PARTITION BY k) FROM t_hash_window_other_partitions SETTINGS query_plan_window_functions_hash_partitioning = 1)
WHERE explain LIKE '%Hash partitioning%' OR explain LIKE '%Skip scatter by partition%';
SELECT (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(x) OVER (PARTITION BY k) AS s FROM t_hash_window_other_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(x) OVER (PARTITION BY k) AS s FROM t_hash_window_other_partitions) SETTINGS query_plan_window_functions_hash_partitioning = 1);

-- Sort limits keep the scatter.
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT k, sum(x) OVER (PARTITION BY k) FROM t_hash_window_partitions SETTINGS query_plan_window_functions_hash_partitioning = 1, max_rows_to_sort = 1000000)
WHERE explain LIKE '%Hash partitioning%' OR explain LIKE '%Skip scatter by partition%';

-- { echoOff }

DROP TABLE t_hash_window_partitions;
DROP TABLE t_hash_window_other_partitions;
