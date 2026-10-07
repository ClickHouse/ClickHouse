-- A read-in-order merge join must NOT be scattered by `parallel_full_sorting_merge`. When both sides read
-- a MergeTree in order of the join key, the pre-join sort is a read-in-order `FinishSorting` (already sorted
-- by the key). An order-preserving scatter of such a side into the per-shard merges can deadlock the
-- pipeline (single-chunk ports; the scatter does not consume new input until all partition chunks of the
-- previous one are pushed, while the per-partition merges and per-shard merge joins consume selectively -
-- two such scatters form a circular wait), so the join falls back to a single merge join, exactly like
-- `full_sorting_merge`, keeping the low-cost in-order read and its virtual rows intact. The result must
-- still match the `hash` algorithm. To run an in-order join shard-by-shard, use the source-side sharding by
-- primary-key ranges (`query_plan_join_shard_by_pk_ranges`), which needs no shuffle.

DROP TABLE IF EXISTS pfsmj_rio_left;
DROP TABLE IF EXISTS pfsmj_rio_right;

CREATE TABLE pfsmj_rio_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_rio_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

-- Several parts per side so each read produces multiple in-order streams. Duplicate ids exercise
-- many-to-many matches.
INSERT INTO pfsmj_rio_left SELECT number % 30000, number FROM numbers(0, 40000);
INSERT INTO pfsmj_rio_left SELECT number % 30000, number FROM numbers(40000, 40000);
INSERT INTO pfsmj_rio_right SELECT number % 20000, number * 2 FROM numbers(0, 50000);
INSERT INTO pfsmj_rio_right SELECT number % 20000, number * 3 FROM numbers(50000, 50000);

-- The join is on `id`, the tables' ORDER BY key, so read-in-order applies: the pre-join sorts are
-- `FinishSorting`, not plain full sorts, and the sharded rewrite must NOT fire (no
-- `ScatterByPartitionTransform`). `optimize_read_in_order = 1` keeps the read in order and
-- `query_plan_join_shard_by_pk_ranges = 0` keeps the PK-range sharding path (which parallelizes a
-- read-in-order join differently, without a scatter) out of the picture. Both settings are randomized in
-- CI, so pin them here.
SELECT 'read_in_order_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0);

-- ...and the fallback single merge join must not redo the sort from scratch: the read-in-order sides only
-- merge (`MergingSortedTransform`) and finish the sort, so there is no `MergeSortingTransform`.
SELECT 'read_in_order_not_resorted', countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0);

-- Contrast: with the in-order read disabled the pre-join sorts are plain full sorts, which ARE scattered
-- (one `ScatterByPartitionTransform` per side) - proving the previous check verifies the pre-sorted-side
-- fallback and not an unrelated failure to shard.
SELECT 'unordered_is_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 0, query_plan_join_shard_by_pk_ranges = 0);

-- Correctness against `hash` for every join kind.
SELECT 'inner',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'left',
    (SELECT (sum(l.a), count()) FROM pfsmj_rio_left AS l LEFT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a), count()) FROM pfsmj_rio_left AS l LEFT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'right',
    (SELECT (sum(r.b), count()) FROM pfsmj_rio_left AS l RIGHT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(r.b), count()) FROM pfsmj_rio_left AS l RIGHT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full',
    (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full_use_nulls',
    (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', join_use_nulls = 1)
  = (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash', join_use_nulls = 1);

-- Virtual rows (read_in_order_use_virtual_row) stay intact on the non-scattered in-order sides; results
-- must match with the optimization both off and on, and with per-block virtual rows on.
SELECT 'virtual_row_off',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', read_in_order_use_virtual_row = 0)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'virtual_row_on',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', read_in_order_use_virtual_row = 1)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'virtual_row_per_block_on',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

-- Row-level check: the two result sets must be identical, not just their aggregates.
SELECT 'rows_identical', count() = 0
FROM
(
    (SELECT l.id, l.a, r.b FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
    EXCEPT
    (SELECT l.id, l.a, r.b FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash')
);

DROP TABLE pfsmj_rio_left;
DROP TABLE pfsmj_rio_right;
