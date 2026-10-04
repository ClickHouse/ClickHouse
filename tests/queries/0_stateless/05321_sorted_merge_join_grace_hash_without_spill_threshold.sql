-- Without a spill threshold `grace_hash` cannot run, and `join_algorithm` is a preference list, so the
-- selection falls through to the next listed algorithm. The join runtime filter pass must see that too:
-- it must not treat such a `grace_hash` as the selected hash-family algorithm, plant a filter, and erase
-- `sorted_merge` from the list - that would leave a standalone `grace_hash`, which refuses to run.

DROP TABLE IF EXISTS smj_gh_left;
DROP TABLE IF EXISTS smj_gh_right;

CREATE TABLE smj_gh_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE smj_gh_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO smj_gh_left SELECT number, number FROM numbers(100000);
INSERT INTO smj_gh_right SELECT number, number * 2 FROM numbers(1000);

SET enable_analyzer = 1;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0,
    query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
    query_plan_optimize_join_order_limit = 1, explain_query_plan_default = 'legacy';
SET legacy_join_size_limits_trigger_spilling = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT '--- grace_hash,sorted_merge: merge join, no runtime filter ---';

SET join_algorithm = 'grace_hash,sorted_merge';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_gh_left AS l INNER JOIN smj_gh_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT count() FROM smj_gh_left AS l INNER JOIN smj_gh_right AS r ON l.id = r.id;

SELECT '--- grace_hash,full_sorting_merge: merge join, no runtime filter ---';

SET join_algorithm = 'grace_hash,full_sorting_merge';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_gh_left AS l INNER JOIN smj_gh_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT count() FROM smj_gh_left AS l INNER JOIN smj_gh_right AS r ON l.id = r.id;

SELECT '--- grace_hash,hash: hash with a runtime filter ---';

SET join_algorithm = 'grace_hash,hash';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_gh_left AS l INNER JOIN smj_gh_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT count() FROM smj_gh_left AS l INNER JOIN smj_gh_right AS r ON l.id = r.id;

DROP TABLE smj_gh_left;
DROP TABLE smj_gh_right;
