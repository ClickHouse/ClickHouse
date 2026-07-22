-- Tags: no-parallel-replicas
-- Assertions on plan shape do not hold with parallel replicas.

-- `parallel_full_sorting_merge` must not shard a side whose read-in-order plan relies on virtual rows.
-- With `query_plan_read_in_order_through_join`, a merge-join pre-sort can read in order *through* a nested
-- join (here: a view joining with `hash`, which preserves the left stream's order). For a non-`LEFT ANY/ALL`
-- nested join, `optimizeReadInOrder` admits that in-order read only when the source emits virtual rows -
-- without them the downstream merge cannot cheaply pick the next input stream once the nested join filters
-- rows out, and can read an excessive amount of data. The scattered rewrite clears virtual rows on a
-- `FinishSorting` side (the scatter cannot pass them through), so such a side must not be sharded at all:
-- the join must fall back to a single merge join with the virtual rows intact, like `full_sorting_merge`.

-- The nested join must not use delayed blocks (external-join spilling) and must preserve the left
-- stream's order, or `optimizeReadInOrder` refuses to read in order through it in the first place.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET query_plan_join_swap_table = 0, enable_join_runtime_filters = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1;
SET query_plan_join_shard_by_pk_ranges = 0, max_threads = 4;

DROP TABLE IF EXISTS pfsmj_vr_a;
DROP TABLE IF EXISTS pfsmj_vr_b;
DROP TABLE IF EXISTS pfsmj_vr_c;
DROP VIEW IF EXISTS pfsmj_vr_nested;

CREATE TABLE pfsmj_vr_a (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_vr_b (id UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_vr_c (id UInt64, u UInt64) ENGINE = MergeTree ORDER BY id;

-- Several parts per table so the in-order read produces multiple streams (virtual rows matter only then).
INSERT INTO pfsmj_vr_a SELECT number, number FROM numbers(0, 5000);
INSERT INTO pfsmj_vr_a SELECT number, number FROM numbers(5000, 5000);
-- The nested join is selective: only every 10th row of `pfsmj_vr_a` survives it.
INSERT INTO pfsmj_vr_b SELECT number * 10, number FROM numbers(1000);
INSERT INTO pfsmj_vr_c SELECT number * 2, number FROM numbers(5000);

-- The nested join is pinned to `hash` (preserves the left stream's order, so read-in-order can go through
-- it) inside the view, independently of the outer query's `join_algorithm`.
CREATE VIEW pfsmj_vr_nested AS
    SELECT a.id AS id, a.v AS v
    FROM pfsmj_vr_a AS a INNER JOIN pfsmj_vr_b AS b ON a.id = b.id
    SETTINGS join_algorithm = 'hash';

-- With virtual rows enabled, the left pre-sort becomes a read-in-order `FinishSorting` through the nested
-- inner join, and its virtual rows are load-bearing - the sharded rewrite must NOT fire.
SELECT 'virtual_row_through_join_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM
(
    EXPLAIN PIPELINE
    SELECT n.v, c.u FROM pfsmj_vr_nested AS n INNER JOIN pfsmj_vr_c AS c ON n.id = c.id
    SETTINGS join_algorithm = 'parallel_full_sorting_merge'
);

-- Sanity: the same query without the nested join (plain tables on both sides) is still scattered, so the
-- previous check verifies the guard and not an unrelated failure to shard.
SELECT 'plain_sides_still_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM
(
    EXPLAIN PIPELINE
    SELECT a.v, c.u FROM pfsmj_vr_a AS a INNER JOIN pfsmj_vr_c AS c ON a.id = c.id
    SETTINGS join_algorithm = 'parallel_full_sorting_merge'
);

-- Correctness: the fallback single merge join must still produce the same result as `hash`.
SELECT 'result_matches_hash',
    (SELECT (sum(n.v + c.u), count()) FROM pfsmj_vr_nested AS n INNER JOIN pfsmj_vr_c AS c ON n.id = c.id
     SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(n.v + c.u), count()) FROM pfsmj_vr_nested AS n INNER JOIN pfsmj_vr_c AS c ON n.id = c.id
     SETTINGS join_algorithm = 'hash');

DROP VIEW pfsmj_vr_nested;
DROP TABLE pfsmj_vr_a;
DROP TABLE pfsmj_vr_b;
DROP TABLE pfsmj_vr_c;
