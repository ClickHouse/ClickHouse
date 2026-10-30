-- A window `PARTITION BY` a constant scatters all rows into one stream, so the constant partition key is
-- determined by an empty key set. Aggregation without keys still emits a row for every stream and must
-- merge them.
SET max_threads = 4, max_block_size = 100, enable_parallel_replicas = 0;
SET allow_aggregate_partitions_independently = 1, max_rows_to_group_by = 0;
SET query_plan_enable_multithreading_after_window_functions = 0;

SELECT countIf(explain LIKE '%Skip merging: 1%')
FROM (EXPLAIN actions = 1 SELECT sum(r) FROM (SELECT count() OVER (PARTITION BY 'c') AS r FROM numbers_mt(1000)));

SELECT sum(r), count() FROM (SELECT count() OVER (PARTITION BY 'c') AS r FROM numbers_mt(1000));
SELECT count() FROM (SELECT number, count() OVER (PARTITION BY 'c' ORDER BY number) FROM numbers_mt(1000));
SELECT sum(r) FROM (SELECT count() OVER (PARTITION BY 'c') AS r FROM numbers_mt(1000) WHERE number > 1000);
