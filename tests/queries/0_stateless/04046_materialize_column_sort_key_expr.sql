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
