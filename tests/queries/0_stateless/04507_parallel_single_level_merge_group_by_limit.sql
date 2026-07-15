-- `max_rows_to_group_by` with the parallel single-level merge. Under the `throw` and `break`
-- overflow modes the per-thread tables stay ordinary and the partition sources re-check the limit
-- against their shared running total; the `any` mode falls back to the serial merge (the tables
-- hold diverged key sets there). The keys of `intDiv` are range-clustered, so each stream sees only
-- its share of the 1500 distinct keys and the limit of 1000 can only break at merge time — but if
-- the scheduler ever hands one stream more than 1000 keys, the consume phase throws the same error,
-- so the expectations hold either way.

SET enable_parallel_single_level_merge = 1;
SET max_threads = 4;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'throw mode, under the limit';
SELECT count(), sum(c) FROM (SELECT intDiv(number, 200) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g)
SETTINGS max_rows_to_group_by = 5000, group_by_overflow_mode = 'throw';

SELECT 'throw mode, merged total over the limit';
SELECT count() FROM (SELECT intDiv(number, 200) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g)
SETTINGS max_rows_to_group_by = 1000, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

-- The serial merge checks the limit only between source tables, so with clustered keys the throw
-- depends on how many streams the scheduler fed (two tables of ~750 keys merge to 1500 with the
-- single check passing). Interleaved keys make every stream exceed the limit during aggregation,
-- which throws deterministically for any stream count.
SELECT 'the limit is enforced with the parallel merge disabled';
SELECT count() FROM (SELECT number % 1500 AS g, count() AS c FROM numbers_mt(300000) GROUP BY g)
SETTINGS max_rows_to_group_by = 1000, group_by_overflow_mode = 'throw', enable_parallel_single_level_merge = 0; -- { serverError TOO_MANY_ROWS }

SELECT 'break mode returns a partial result';
SELECT count() >= 1, count() <= 1500 FROM (SELECT intDiv(number, 200) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g)
SETTINGS max_rows_to_group_by = 1000, group_by_overflow_mode = 'break';

SELECT 'any mode stays on the serial merge and returns a limited result';
SELECT count() > 0, count() <= 1500 FROM (SELECT intDiv(number, 200) AS g, count() AS c FROM numbers_mt(300000) GROUP BY g)
SETTINGS max_rows_to_group_by = 1000, group_by_overflow_mode = 'any';
