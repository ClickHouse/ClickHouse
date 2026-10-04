-- Tags: no-random-merge-tree-settings
-- Regression test for `Join is supported only for pipelines with one output port, got N and M` (a
-- logical error) with `parallel_sorted_merge`. The algorithm shards the join by primary-key ranges at
-- plan time, but a data-dependent `PREWHERE` can prune one side down to a single empty stream at
-- pipeline-building time (the empty-parts shortcut of `ReadFromMergeTree`), so the stream counts
-- diverge. `JoinStep` then falls back from the by-shards pipeline to the plain single-stream merge
-- join, merging each side's per-shard streams (sorted by the join keys) back into one sorted stream.

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
-- side is fully pruned to a single stream while the sharded right side keeps several streams. That
-- divergence used to raise the logical error; it must return 0 rows now.
SELECT count() FROM psmj_div AS a ALL INNER JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000;

-- Non-empty output through the fallback: a `RIGHT` join with `PREWHERE a.c0 > 1000` prunes the left
-- (`a`) side to a single empty stream while the sharded right (`b`) side keeps several non-empty
-- per-shard streams. Every merged `b` row must reach the output: a row that the merge drops or
-- duplicates changes the count or the checksum. Compare against the hash join, which does not use this
-- pipeline. Must be 1.
SELECT
    (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM psmj_div AS a ALL RIGHT JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000)
  = (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM psmj_div AS a ALL RIGHT JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000 SETTINGS join_algorithm = 'hash');

-- The divergence only arises when the `PREWHERE` prunes one side to zero rows: with any surviving row
-- both sides stay sharded and equal, so the results above cannot distinguish the sorted merge from an
-- unordered `resize(1)`. Assert the sorted merge in the plan instead. Must be 1.
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT count() FROM psmj_div AS a ALL RIGHT JOIN psmj_div AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000
) WHERE explain ILIKE '%MergingSortedTransform%';

DROP TABLE psmj_div;
