-- A merge join emits its rows sorted by its join keys, so a join above it on (a prefix of) those keys needs
-- no re-sort: the eligibility of `sorted_merge` / `parallel_sorted_merge` sees through nested merge joins,
-- and a multi-way join on a shared key stays a chain of streaming merge joins with
-- `join_algorithm = 'sorted_merge,hash'` - instead of only the bottom join being a merge join and every
-- join above it falling through to the hash algorithm (https://github.com/ClickHouse/ClickHouse/issues/117680).

DROP TABLE IF EXISTS mwj_a;
DROP TABLE IF EXISTS mwj_b;
DROP TABLE IF EXISTS mwj_c;
DROP TABLE IF EXISTS mwj_d;

CREATE TABLE mwj_a (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mwj_b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mwj_c (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mwj_d (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

-- Several parts per table so each read produces multiple in-order streams; duplicates, gaps and partial
-- overlaps of the key ranges exercise many-to-many matches and unmatched rows on every side.
INSERT INTO mwj_a SELECT number, number FROM numbers(0, 20000);
INSERT INTO mwj_a SELECT number, number * 2 FROM numbers(20000, 20000);
INSERT INTO mwj_b SELECT number % 30000, number FROM numbers(0, 45000);
INSERT INTO mwj_b SELECT number % 30000, number * 3 FROM numbers(45000, 15000);
INSERT INTO mwj_c SELECT number * 2, number FROM numbers(0, 25000);
INSERT INTO mwj_d SELECT number % 10000, number FROM numbers(0, 30000);

-- The eligibility is decided on the query plan, which exists only for the analyzer.
SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on: the in-order read must be allowed, the
-- PK-range sharding stays out of the picture unless asked for, the join order and sides stay as written, and
-- the reads are local.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, enable_parallel_replicas = 0;
SET max_threads = 4;

-- Three tables joined on the shared primary key: both joins are merge joins (no hash join in the pipeline)
-- and nothing is sorted from scratch (no `MergeSortingTransform`).
SELECT 'chain_of_3_all_merge', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%FillingRightJoinSide%') = 0, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- Every pre-join sort in the plan is a `FinishSorting` (has a prefix), including the ones over the nested join.
SELECT 'chain_of_3_all_sorts_prefixed', countIf(explain LIKE '%Prefix sort description%') = 4, countIf(explain LIKE '%Sort description%' AND explain NOT LIKE '%Prefix sort description%' AND explain NOT LIKE '%Result sort description%') = 0
FROM (EXPLAIN PLAN SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- Four tables: the chain keeps growing without a single re-sort.
SELECT 'chain_of_4_all_merge', countIf(explain LIKE '%MergeJoinTransform%') = 3, countIf(explain LIKE '%FillingRightJoinSide%') = 0, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) + sum(d.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id INNER JOIN mwj_d AS d ON a.id = d.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- The rows of an INNER merge join carry equal left and right keys, so the join above may refer to either.
SELECT 'inner_right_key_sorted', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- A LEFT merge join is sorted by its left keys only: the right key of an unmatched row is a default.
SELECT 'left_left_key_sorted', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');
SELECT 'left_right_key_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 1, countIf(explain LIKE '%FillingRightJoinSide%') >= 1
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- ...and a RIGHT merge join by its right keys only.
SELECT 'right_right_key_sorted', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a RIGHT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');
SELECT 'right_left_key_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 1, countIf(explain LIKE '%FillingRightJoinSide%') >= 1
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a RIGHT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- A FULL merge join interleaves the unmatched rows of both sides, so no column of its output is sorted.
SELECT 'full_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 1, countIf(explain LIKE '%FillingRightJoinSide%') >= 1
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a FULL JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- A key computed from the nested join's output inherits no order.
SELECT 'computed_key_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 1, countIf(explain LIKE '%FillingRightJoinSide%') >= 1
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id + 1 = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- The other side of the join above must still be readable in order on its own.
SELECT 'other_side_unsorted_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 1, countIf(explain LIKE '%FillingRightJoinSide%') >= 1
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.v SETTINGS join_algorithm = 'sorted_merge,hash');

-- A nested join that fell through to `hash` provides no order either: the join above falls through too.
SELECT 'hash_below_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.v = b.v INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- The always-available `full_sorting_merge` benefits the same way: no re-sort above a nested merge join.
SELECT 'full_sorting_merge_no_resort', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'full_sorting_merge');

-- `sorted_merge` alone is enough for the whole chain.
SELECT 'alone_on_chain', count() > 0 FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge';

-- `parallel_sorted_merge`: the whole chain is sharded by the shared primary-key ranges (a `Sharding:` marker
-- on both joins), still with no re-sort.
SELECT 'parallel_chain_sharded', countIf(explain LIKE '%Sharding:%') = 2
FROM (EXPLAIN PLAN SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash');
SELECT 'parallel_chain_no_resort', countIf(explain LIKE '%MergeJoinTransform%') >= 2, countIf(explain LIKE '%FillingRightJoinSide%') = 0, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash');

-- When only the nested join can be sharded (the key of the join above is not the leftmost table's primary
-- key), the join above still runs as a merge join over the sharded one.
SELECT 'parallel_chain_partially_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN PLAN SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash');

-- Correctness against `hash` for the shapes above (the pipeline checks pin that the merge algorithm really is
-- the one executing).
SELECT 'inner_chain',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'hash');

SELECT 'inner_chain_of_4',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), sum(d.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id INNER JOIN mwj_d AS d ON a.id = d.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), sum(d.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id INNER JOIN mwj_d AS d ON a.id = d.id SETTINGS join_algorithm = 'hash');

SELECT 'inner_right_key',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'hash');

SELECT 'left_then_inner',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'hash');

SELECT 'left_then_left',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id LEFT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id LEFT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'hash');

SELECT 'right_then_inner',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a RIGHT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a RIGHT JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'hash');

SELECT 'inner_then_right',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id RIGHT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id RIGHT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'hash');

SELECT 'inner_chain_use_nulls',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id LEFT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash', join_use_nulls = 1)
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id LEFT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'hash', join_use_nulls = 1);

SELECT 'parallel_inner_chain',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), sum(d.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id INNER JOIN mwj_d AS d ON a.id = d.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), sum(d.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id INNER JOIN mwj_d AS d ON a.id = d.id SETTINGS join_algorithm = 'hash');

SELECT 'parallel_left_chain',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id LEFT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a LEFT JOIN mwj_b AS b ON a.id = b.id LEFT JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'hash');

SELECT 'parallel_partially_sharded',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON b.id = c.id SETTINGS join_algorithm = 'hash');

-- A filter between the joins keeps the order (it only drops rows).
SELECT 'filter_between_joins_merge', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(ab.av) + sum(ab.bv) + sum(c.v) FROM (SELECT a.id AS id, a.v AS av, b.v AS bv FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id WHERE a.v + b.v > 100) AS ab INNER JOIN mwj_c AS c ON ab.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash');

SELECT 'filter_between_joins',
    (SELECT (sum(ab.av), sum(ab.bv), sum(c.v), count()) FROM (SELECT a.id AS id, a.v AS av, b.v AS bv FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id WHERE a.v + b.v > 100) AS ab INNER JOIN mwj_c AS c ON ab.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(ab.av), sum(ab.bv), sum(c.v), count()) FROM (SELECT a.id AS id, a.v AS av, b.v AS bv FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id WHERE a.v + b.v > 100) AS ab INNER JOIN mwj_c AS c ON ab.id = c.id SETTINGS join_algorithm = 'hash');

-- With parallel replicas the reads are distributed and no input is readable in order, so the whole chain falls
-- through to `hash`; in that mode the eligibility of a join above is predicted while the join below is not yet
-- physical (`predictMergeJoinOutputOrder`). The result is the same.
SELECT 'parallel_replicas_chain',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash', enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0)
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM mwj_a AS a INNER JOIN mwj_b AS b ON a.id = b.id INNER JOIN mwj_c AS c ON a.id = c.id SETTINGS join_algorithm = 'hash', enable_parallel_replicas = 0);

DROP TABLE mwj_a;
DROP TABLE mwj_b;
DROP TABLE mwj_c;
DROP TABLE mwj_d;
