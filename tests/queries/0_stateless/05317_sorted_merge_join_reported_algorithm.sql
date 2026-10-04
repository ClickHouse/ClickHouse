-- `sorted_merge` and `parallel_sorted_merge` build the same `FullSortingMergeJoin` object as
-- `full_sorting_merge`, so `system.query_log.used_join_algorithms` must report the variant that was
-- selected and actually ran, not `FULL_SORTING_MERGE`. The `parallel_sorted_merge` variant is reported
-- only when the join was built sharded (`JoinStep` degrades it to a single-stream `sorted_merge` if the
-- stream counts of the two sides diverge at pipeline-building time).

DROP TABLE IF EXISTS smj_rep_left;
DROP TABLE IF EXISTS smj_rep_right;

-- Small `index_granularity` so that the modest row counts still produce enough granules for the
-- primary-key-range sharding to split both inputs into several layers.
CREATE TABLE smj_rep_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;
CREATE TABLE smj_rep_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

INSERT INTO smj_rep_left SELECT number % 3750, number FROM numbers(0, 5000);
INSERT INTO smj_rep_left SELECT number % 3750, number FROM numbers(5000, 5000);
INSERT INTO smj_rep_right SELECT number % 2500, number * 2 FROM numbers(0, 6250);
INSERT INTO smj_rep_right SELECT number % 2500, number * 3 FROM numbers(6250, 6250);

-- The eligibility of these algorithms is decided on the query plan, which exists only for the analyzer.
SET enable_analyzer = 1;
SET log_queries = 1;
-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;
SET max_threads = 4;

SELECT count() FROM smj_rep_left AS l INNER JOIN smj_rep_right AS r ON l.id = r.id
FORMAT Null SETTINGS log_comment = '05317_sorted_merge', join_algorithm = 'sorted_merge';

SELECT count() FROM smj_rep_left AS l INNER JOIN smj_rep_right AS r ON l.id = r.id
FORMAT Null SETTINGS log_comment = '05317_parallel_sorted_merge', join_algorithm = 'parallel_sorted_merge';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_join_algorithms
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05317_sorted_merge', '05317_parallel_sorted_merge')
ORDER BY log_comment;

-- `EXPLAIN ANALYZE` describes the join that ran, and it ran sharded.
SELECT 'executed_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN ANALYZE SELECT count() FROM smj_rep_left AS l INNER JOIN smj_rep_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge');

DROP TABLE smj_rep_left;
DROP TABLE smj_rep_right;
