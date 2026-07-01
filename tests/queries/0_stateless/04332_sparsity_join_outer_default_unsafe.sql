-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

-- Regression test: when the query has a JOIN, sparsity pruning must not push
-- a `WHERE` conjunct below the join. An outer join materialises unmatched rows
-- with type-default column values; pruning a part because it has no rows where
-- `right.v = 0` would then turn formerly-matched left rows into unmatched ones,
-- they would receive the default `right.v = 0`, and the post-join `WHERE` would
-- start passing rows that should have been rejected.

DROP TABLE IF EXISTS t_join_outer_default_left;
DROP TABLE IF EXISTS t_join_outer_default_right;

CREATE TABLE t_join_outer_default_left (k UInt64)
ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE t_join_outer_default_right (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_join_outer_default_left  VALUES (1), (2), (3);
-- All non-default values: `num_defaults(v) = 0`, so the pruner would otherwise
-- treat any conjunct of the form `v = 0` as contradicted on this part.
INSERT INTO t_join_outer_default_right VALUES (1, 10), (2, 11);

SET join_use_nulls = 0;

SELECT 'planning', l.k, r.v
FROM t_join_outer_default_left l LEFT JOIN t_join_outer_default_right r ON l.k = r.k
WHERE r.v = 0
ORDER BY l.k
SETTINGS use_sparsity_info_for_pruning = 'planning';

SELECT 'data_read', l.k, r.v
FROM t_join_outer_default_left l LEFT JOIN t_join_outer_default_right r ON l.k = r.k
WHERE r.v = 0
ORDER BY l.k
SETTINGS use_sparsity_info_for_pruning = 'data_read';

-- `ARRAY JOIN` can wrap the `JOIN` in the same join tree, so the root of the
-- join tree is `ARRAY_JOIN` rather than `JOIN`. The opt-out must still trigger.
SELECT 'planning_with_array_join', l.k, r.v, a
FROM t_join_outer_default_left l LEFT JOIN t_join_outer_default_right r ON l.k = r.k
ARRAY JOIN [1] AS a
WHERE r.v = 0
ORDER BY l.k
SETTINGS use_sparsity_info_for_pruning = 'planning';

SELECT 'data_read_with_array_join', l.k, r.v, a
FROM t_join_outer_default_left l LEFT JOIN t_join_outer_default_right r ON l.k = r.k
ARRAY JOIN [1] AS a
WHERE r.v = 0
ORDER BY l.k
SETTINGS use_sparsity_info_for_pruning = 'data_read';

DROP TABLE t_join_outer_default_left;
DROP TABLE t_join_outer_default_right;
