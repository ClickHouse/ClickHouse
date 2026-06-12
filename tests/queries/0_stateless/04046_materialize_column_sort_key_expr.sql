-- Materializing a column used in a sort key expression should be refused
-- See https://github.com/ClickHouse/ClickHouse/issues/93139

-- Case 1: Direct sort key column (already blocked)
DROP TABLE IF EXISTS t_mat_sort_direct;
CREATE TABLE t_mat_sort_direct (a Int, b Int MATERIALIZED a + 1) ENGINE = MergeTree() ORDER BY b;
ALTER TABLE t_mat_sort_direct MATERIALIZE COLUMN b; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_sort_direct;

-- Case 2: Column used inside sort key function expression (was NOT blocked — the bug)
DROP TABLE IF EXISTS t_mat_sort_expr;
CREATE TABLE t_mat_sort_expr (c1 Int, c2 DateTime MATERIALIZED now()) ENGINE = MergeTree() ORDER BY (metroHash64(c1, c2));
ALTER TABLE t_mat_sort_expr MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_sort_expr;

-- Case 3: Parent column whose subcolumn is in the sort key (ORDER BY t.k) — must be blocked
DROP TABLE IF EXISTS t_mat_sort_subcolumn;
CREATE TABLE t_mat_sort_subcolumn (a Int, t Tuple(k UInt64, v UInt64) MATERIALIZED (rand64(), a)) ENGINE = MergeTree() ORDER BY t.k;
ALTER TABLE t_mat_sort_subcolumn MATERIALIZE COLUMN t; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_sort_subcolumn;

-- Case 4: Column NOT in sort key — should succeed
DROP TABLE IF EXISTS t_mat_sort_ok;
CREATE TABLE t_mat_sort_ok (a Int, b Int MATERIALIZED a + 1, c Int) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_sort_ok (a, c) VALUES (1, 10);
ALTER TABLE t_mat_sort_ok MATERIALIZE COLUMN b;
SELECT b FROM t_mat_sort_ok;
DROP TABLE t_mat_sort_ok;

-- Case 5: Column used in a projection's sort key expression — must be blocked too,
-- otherwise the already-materialized projection parts would keep stale sort order.
DROP TABLE IF EXISTS t_mat_sort_projection;
CREATE TABLE t_mat_sort_projection
(
    a Int,
    c2 DateTime MATERIALIZED now(),
    PROJECTION p (SELECT * ORDER BY metroHash64(c2))
) ENGINE = MergeTree() ORDER BY a;
ALTER TABLE t_mat_sort_projection MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_sort_projection;

-- Case 6: Column used in a partition key function expression — must be blocked.
-- Materializing it would move existing rows to a different partition while the part
-- metadata still describes the old partition id.
DROP TABLE IF EXISTS t_mat_part_expr;
CREATE TABLE t_mat_part_expr (a Int, c2 DateTime MATERIALIZED now()) ENGINE = MergeTree() PARTITION BY toYYYYMM(c2) ORDER BY a;
ALTER TABLE t_mat_part_expr MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_part_expr;

-- Case 7: Parent column whose subcolumn is in the partition key — must be blocked.
DROP TABLE IF EXISTS t_mat_part_subcolumn;
CREATE TABLE t_mat_part_subcolumn (a Int, t Tuple(k UInt64, v UInt64) MATERIALIZED (rand64(), a)) ENGINE = MergeTree() PARTITION BY t.k ORDER BY a;
ALTER TABLE t_mat_part_subcolumn MATERIALIZE COLUMN t; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_part_subcolumn;

-- Case 8: Materializing a column with a dependent skip index (the column is neither in the
-- sorting key nor the partition key) is allowed, but the index must be rebuilt so it does not
-- keep stale min/max values. We change the expression first so MATERIALIZE COLUMN recomputes
-- the on-disk values; if the minmax index were hardlinked instead of rebuilt, the query for the
-- new value would be wrongly pruned and return 0 rows.
DROP TABLE IF EXISTS t_mat_index_rebuild;
CREATE TABLE t_mat_index_rebuild
(
    a Int,
    c2 Int MATERIALIZED a * 10,
    INDEX idx_c2 c2 TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_mat_index_rebuild (a) SELECT number FROM numbers(10);
ALTER TABLE t_mat_index_rebuild MODIFY COLUMN c2 Int MATERIALIZED a * 100;
ALTER TABLE t_mat_index_rebuild MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT c2 FROM t_mat_index_rebuild ORDER BY a;
SELECT count() FROM t_mat_index_rebuild WHERE c2 = 500 SETTINGS force_data_skipping_indices = 'idx_c2';
SELECT count() FROM t_mat_index_rebuild WHERE c2 = 50 SETTINGS force_data_skipping_indices = 'idx_c2';
DROP TABLE t_mat_index_rebuild;

-- Case 9: Materializing a column referenced by a projection whose ORDER BY does NOT mention it
-- is allowed, but the projection must be rebuilt so it does not serve stale values.
DROP TABLE IF EXISTS t_mat_proj_rebuild;
CREATE TABLE t_mat_proj_rebuild
(
    a Int,
    c2 Int MATERIALIZED a * 10,
    PROJECTION p (SELECT a, c2 ORDER BY a)
) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_proj_rebuild (a) SELECT number FROM numbers(5);
ALTER TABLE t_mat_proj_rebuild MODIFY COLUMN c2 Int MATERIALIZED a * 100;
ALTER TABLE t_mat_proj_rebuild MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT a, c2 FROM t_mat_proj_rebuild ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_mat_proj_rebuild;

-- Case 10: Skip index over a multi-column expression (a + c2). Materializing c2 must feed the
-- sibling column `a` into the mutation stream so the index can be rebuilt; otherwise the rebuild
-- would read a block that is missing `a`. We change the expression first so the recomputed values
-- differ, then query for a value that only exists after materialization: with a stale (hardlinked)
-- index it would be wrongly pruned.
DROP TABLE IF EXISTS t_mat_index_multicol;
CREATE TABLE t_mat_index_multicol
(
    a Int,
    c2 Int MATERIALIZED a * 10,
    INDEX idx_sum (a + c2) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_mat_index_multicol (a) SELECT number FROM numbers(10);
ALTER TABLE t_mat_index_multicol MODIFY COLUMN c2 Int MATERIALIZED a * 100;
ALTER TABLE t_mat_index_multicol MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT count() FROM t_mat_index_multicol WHERE (a + c2) = 505 SETTINGS force_data_skipping_indices = 'idx_sum';
SELECT count() FROM t_mat_index_multicol WHERE (a + c2) = 55 SETTINGS force_data_skipping_indices = 'idx_sum';
DROP TABLE t_mat_index_multicol;

-- Case 11: A stored MATERIALIZED column computed from the materialized column is itself the
-- sorting key. Recomputing it would break the sort order, so the command must be refused.
DROP TABLE IF EXISTS t_mat_dep_sort_key;
CREATE TABLE t_mat_dep_sort_key (a Int, c2 Int MATERIALIZED a * 10, k Int MATERIALIZED c2 + 1)
    ENGINE = MergeTree() ORDER BY k;
ALTER TABLE t_mat_dep_sort_key MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_dep_sort_key;

-- Case 12: A stored MATERIALIZED column computed from the materialized column is in the
-- partition key — must be refused for the same reason as the direct partition key case.
DROP TABLE IF EXISTS t_mat_dep_part_key;
CREATE TABLE t_mat_dep_part_key (a Int, c2 Int MATERIALIZED a * 10, p Int MATERIALIZED c2 % 2)
    ENGINE = MergeTree() PARTITION BY p ORDER BY a;
ALTER TABLE t_mat_dep_part_key MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_dep_part_key;

-- Case 13: A stored MATERIALIZED column computed from the materialized column is in a
-- projection's sorting key — must be refused like the direct projection sort key case.
DROP TABLE IF EXISTS t_mat_dep_proj_key;
CREATE TABLE t_mat_dep_proj_key
(
    a Int,
    c2 Int MATERIALIZED a * 10,
    m Int MATERIALIZED c2 + 1,
    PROJECTION p (SELECT * ORDER BY m)
) ENGINE = MergeTree() ORDER BY a;
ALTER TABLE t_mat_dep_proj_key MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_dep_proj_key;

-- Case 14: A stored MATERIALIZED column computed from the materialized column, not used in any
-- key — the command is allowed and the dependent column must be recomputed from the new values,
-- otherwise it would keep values derived from the old source column.
DROP TABLE IF EXISTS t_mat_dep_recompute;
CREATE TABLE t_mat_dep_recompute (a Int, c2 Int MATERIALIZED a * 10, m Int MATERIALIZED c2 + 1)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_dep_recompute (a) SELECT number FROM numbers(3);
ALTER TABLE t_mat_dep_recompute MODIFY COLUMN c2 Int MATERIALIZED a * 100;
ALTER TABLE t_mat_dep_recompute MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT a, c2, m FROM t_mat_dep_recompute ORDER BY a;
DROP TABLE t_mat_dep_recompute;

-- Case 15: A skip index over the dependent MATERIALIZED column. The dependent column is
-- recomputed, so the index must be rebuilt as well; with a stale (hardlinked) index the query
-- for a recomputed value would be wrongly pruned.
DROP TABLE IF EXISTS t_mat_dep_index;
CREATE TABLE t_mat_dep_index
(
    a Int,
    c2 Int MATERIALIZED a * 10,
    m Int MATERIALIZED c2 + 1,
    INDEX idx_m m TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_mat_dep_index (a) SELECT number FROM numbers(10);
ALTER TABLE t_mat_dep_index MODIFY COLUMN c2 Int MATERIALIZED a * 100;
ALTER TABLE t_mat_dep_index MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT count() FROM t_mat_dep_index WHERE m = 501 SETTINGS force_data_skipping_indices = 'idx_m';
SELECT count() FROM t_mat_dep_index WHERE m = 51 SETTINGS force_data_skipping_indices = 'idx_m';
DROP TABLE t_mat_dep_index;
