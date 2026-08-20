-- Regression coverage for `parallel_full_sorting_merge` combined with the filter-by-set-on-the-fly
-- optimization (`max_rows_in_set_to_optimize_join > 0`).
--
-- Both planners insert `CreateSetAndFilterOnTheFlyStep` below the pre-join merge-join sorts, so the sharded
-- rewrite still fires on top of it: each side becomes filter-by-set -> scatter by key hash -> per-shard
-- full sorts -> per-shard merge joins. The on-the-fly filter relies on the sorting buffering both streams
-- before emitting any rows (the `ReadHeadBalancedProcessor` ping-pong between the two sides deadlocked with
-- plain `full_sorting_merge` once - see `04007_pipeline_stuck_ping_pong_deadlock.sql` and issue #57728),
-- and the scattered path replaces the single fully-draining merge sort with a scatter plus per-shard sorts,
-- so it needs its own proof that the ping-pong still terminates and the filtered results stay correct.

DROP TABLE IF EXISTS pfsmj_sotf_left;
DROP TABLE IF EXISTS pfsmj_sotf_right;

CREATE TABLE pfsmj_sotf_left (c UInt64) ENGINE = MergeTree ORDER BY c;
CREATE TABLE pfsmj_sotf_right (c UInt64) ENGINE = MergeTree ORDER BY c;

INSERT INTO pfsmj_sotf_left SELECT * FROM numbers(1000000);
-- Only a subset matches, so the on-the-fly set actually filters the left side.
INSERT INTO pfsmj_sotf_right SELECT number * 2 FROM numbers(250000);

SET enable_analyzer = 1;

-- The plan must contain BOTH the on-the-fly set/filter transforms and the two scatters: neither
-- optimization may silently disable the other, otherwise the execution checks below prove nothing.
SELECT 'analyzer set_and_scatter_combined',
    countIf(explain LIKE '%CreatingSetsOnTheFlyTransform%') >= 2,
    countIf(explain LIKE '%FilterBySetOnTheFlyTransform%') >= 1,
    countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT count() FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_rows_in_set_to_optimize_join = 100000000,
           max_threads = 4, query_plan_join_swap_table = 0);

-- The shape from the original ping-pong deadlock (a plain side vs a `LIMIT`-ed sorted subquery,
-- `max_threads = 2`), now under the sharded rewrite: must terminate and return the exact count.
SELECT 'analyzer deadlock_shape', count() = 500000
FROM pfsmj_sotf_left AS t1s,
     (SELECT * FROM pfsmj_sotf_left ORDER BY c ASC LIMIT 500000) AS t2s
WHERE t1s.c = t2s.c
SETTINGS max_rows_in_set_to_optimize_join = 100000000, join_algorithm = 'parallel_full_sorting_merge',
         query_plan_join_swap_table = 0, max_threads = 2;

-- Filtering effective (selective right side) on several threads: results must match the `hash` algorithm.
SELECT 'analyzer filtered_inner',
    (SELECT (count(), sum(l.c)) FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
     SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_rows_in_set_to_optimize_join = 100000000,
              query_plan_join_swap_table = 0, max_threads = 4)
  = (SELECT (count(), sum(l.c)) FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
     SETTINGS join_algorithm = 'hash');

-- A small set limit makes the set overflow mid-query and the filtering degrade gracefully; the result must
-- be unaffected.
SELECT 'analyzer set_overflow',
    (SELECT (count(), sum(l.c)) FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
     SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_rows_in_set_to_optimize_join = 1000,
              query_plan_join_swap_table = 0, max_threads = 4)
  = (SELECT (count(), sum(l.c)) FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
     SETTINGS join_algorithm = 'hash');

-- Legacy planner (`InterpreterSelectQuery`) inserts the on-the-fly steps independently; same combined shape
-- and the same execution checks. `enable_analyzer` cannot be changed inside a subquery, so set it at
-- session level (as in `04494` / `04500`).
SET enable_analyzer = 0;

SELECT 'legacy set_and_scatter_combined',
    countIf(explain LIKE '%CreatingSetsOnTheFlyTransform%') >= 2,
    countIf(explain LIKE '%FilterBySetOnTheFlyTransform%') >= 1,
    countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT count() FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_rows_in_set_to_optimize_join = 100000000,
           max_threads = 4, query_plan_join_swap_table = 0);

SELECT 'legacy deadlock_shape', count() = 500000
FROM pfsmj_sotf_left AS t1s,
     (SELECT * FROM pfsmj_sotf_left ORDER BY c ASC LIMIT 500000) AS t2s
WHERE t1s.c = t2s.c
SETTINGS max_rows_in_set_to_optimize_join = 100000000, join_algorithm = 'parallel_full_sorting_merge',
         query_plan_join_swap_table = 0, max_threads = 2;

SELECT 'legacy filtered_inner',
    (SELECT (count(), sum(l.c)) FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
     SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_rows_in_set_to_optimize_join = 100000000,
              query_plan_join_swap_table = 0, max_threads = 4)
  = (SELECT (count(), sum(l.c)) FROM pfsmj_sotf_left AS l INNER JOIN pfsmj_sotf_right AS r ON l.c = r.c
     SETTINGS join_algorithm = 'hash');

DROP TABLE pfsmj_sotf_left;
DROP TABLE pfsmj_sotf_right;
