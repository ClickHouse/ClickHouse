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
