-- Tags: no-random-merge-tree-settings
-- Regression test for `Join is supported only for pipelines with one output port, got N and M` (a
-- logical error) with `parallel_sorted_merge`. The algorithm shards the join by primary-key ranges at
-- plan time, and a data-dependent `PREWHERE` can prune one side down to no parts at all at
-- pipeline-building time (the empty-parts shortcut of `ReadFromMergeTree`). That side used to collapse
-- to a single empty stream, so the stream counts of the two sides diverged. Now an empty side keeps one
-- output port per shard (`ReadFromMergeTree::getNumStreamsWhenNothingToRead`), the counts stay equal and
-- the join runs sharded. (`JoinStep` still degrades a diverged join to the single-stream merge join as a
-- safety net, but no query shape is known to reach it.)

DROP TABLE IF EXISTS psmj_div;
CREATE TABLE psmj_div (c0 UInt64, c1 UInt64, s String) ENGINE = MergeTree ORDER BY (c0, c1) SETTINGS index_granularity = 8;
INSERT INTO psmj_div SELECT number, number % 10, 'x' FROM numbers(100);
INSERT INTO psmj_div SELECT number + 100, number % 10, 'x' FROM numbers(100);
INSERT INTO psmj_div SELECT number + 200, number % 10, 'x' FROM numbers(100);

-- The eligibility of `parallel_sorted_merge` is decided on the query plan, which exists only for the analyzer.
SET enable_analyzer = 1;
SET join_algorithm = 'parallel_sorted_merge';
SET max_threads = 8;
-- Pin the settings randomized in CI that the plan shape depends on. `query_plan_join_shard_by_pk_ranges`
-- is pinned to its default 0: the sharding under test is the one `parallel_sorted_merge` enables itself.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;

-- `c0 > 1000` is a data-dependent range on the primary key: no part contains such a row, so the left
-- side is fully pruned while the sharded right side keeps several streams. That used to raise the
-- logical error; it must return 0 rows now.
SELECT count() FROM psmj_div AS a ALL INNER JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000;

-- Non-empty output: a `RIGHT` join with `PREWHERE a.c0 > 1000` prunes the left (`a`) side to no parts
-- while the sharded right (`b`) side keeps several non-empty per-shard streams. Every `b` row must reach
-- the output: a row that the join drops or duplicates changes the count or the checksum. Compare against the hash join, which does not use this
-- pipeline. Must be 1.
SELECT
    (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM psmj_div AS a ALL RIGHT JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000)
  = (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM psmj_div AS a ALL RIGHT JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000 SETTINGS join_algorithm = 'hash');

-- The side pruned to zero parts keeps one port per shard, so the join is not degraded: it runs sharded
-- and is reported as `PARALLEL_SORTED_MERGE`, not as a degraded `SORTED_MERGE`. Must be 1.
SELECT countIf(explain LIKE '%Sharding:%') = 1 FROM (
    EXPLAIN ANALYZE SELECT count() FROM psmj_div AS a ALL RIGHT JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000
);

SELECT count() FROM psmj_div AS a ALL RIGHT JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000
FORMAT Null SETTINGS log_queries = 1, log_comment = '04895_pruned_side';

SYSTEM FLUSH LOGS query_log;
SELECT used_join_algorithms
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04895_pruned_side';

DROP TABLE psmj_div;
