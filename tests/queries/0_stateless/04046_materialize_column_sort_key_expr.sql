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

-- Case 16: Materializing a column read by a TTL expression must recalculate the part's TTL bounds,
-- mirroring the UPDATE path. Without this the new part's ttl_infos are copied from the source part
-- and keep stale min/max, so TTL scheduling/deletes/moves would use the old bounds. We change the
-- expression first so the recomputed values differ, then check that the stored delete-TTL bound
-- matches the current (recomputed) c2 value; with a stale (hardlinked) ttl.txt it would not.
-- Both the old and the new bounds are far in the future so no row is ever expired/deleted.
DROP TABLE IF EXISTS t_mat_ttl_recalc;
CREATE TABLE t_mat_ttl_recalc (a Int, c2 DateTime MATERIALIZED toDateTime(1800000000 + a))
    ENGINE = MergeTree() ORDER BY a TTL c2 + INTERVAL 1 DAY;
INSERT INTO t_mat_ttl_recalc (a) VALUES (1);
ALTER TABLE t_mat_ttl_recalc MODIFY COLUMN c2 DateTime MATERIALIZED toDateTime(1900000000 + a);
ALTER TABLE t_mat_ttl_recalc MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT delete_ttl_info_min = (SELECT c2 + INTERVAL 1 DAY FROM t_mat_ttl_recalc LIMIT 1)
    FROM system.parts WHERE table = 't_mat_ttl_recalc' AND active AND database = currentDatabase();
DROP TABLE t_mat_ttl_recalc;

-- Case 17: The CollapsingMergeTree sign column is a merge-semantic key column even when it is not in
-- ORDER BY. UPDATE of it is refused via getKeyColumns; MATERIALIZE COLUMN rewrites it just the same,
-- so it must be refused too — otherwise the collapsing semantics of existing data would be corrupted.
DROP TABLE IF EXISTS t_mat_sign;
CREATE TABLE t_mat_sign (a Int, sign Int8 MATERIALIZED 1) ENGINE = CollapsingMergeTree(sign) ORDER BY a;
ALTER TABLE t_mat_sign MATERIALIZE COLUMN sign; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_sign;

-- Case 18: Same for the ReplacingMergeTree version column.
DROP TABLE IF EXISTS t_mat_version;
CREATE TABLE t_mat_version (a Int, ver UInt32 MATERIALIZED 1) ENGINE = ReplacingMergeTree(ver) ORDER BY a;
ALTER TABLE t_mat_version MATERIALIZE COLUMN ver; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_version;

-- Case 19: A stored MATERIALIZED column computed from the materialized column is itself the engine
-- sign column. Recomputing the source column would recompute the sign, so it must be refused for the
-- same reason as the direct sign-column case.
DROP TABLE IF EXISTS t_mat_dep_sign;
CREATE TABLE t_mat_dep_sign (a Int, c2 Int MATERIALIZED a, s Int8 MATERIALIZED c2) ENGINE = CollapsingMergeTree(s) ORDER BY a;
ALTER TABLE t_mat_dep_sign MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_dep_sign;

-- Case 20: A TTL expression reads a subcolumn of the materialized column (TTL t.k while
-- materializing the parent Tuple column t). Recalculating the part's TTL bounds is not supported
-- for subcolumn dependencies (unlike a full-column TTL as in Case 16), so — following the same
-- fail-close approach used for key columns — the command is refused rather than leaving stale
-- ttl_infos copied from the source part.
DROP TABLE IF EXISTS t_mat_ttl_subcolumn;
CREATE TABLE t_mat_ttl_subcolumn (a Int, t Tuple(k DateTime, v UInt64) MATERIALIZED (toDateTime(1800000000 + a), 0))
    ENGINE = MergeTree() ORDER BY a TTL t.k + INTERVAL 1 DAY;
ALTER TABLE t_mat_ttl_subcolumn MATERIALIZE COLUMN t; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_subcolumn;

-- Case 21: Same as Case 20, but the TTL reads a *dynamic* subcolumn — a JSON path (TTL j.d while
-- materializing the parent JSON column j). `IDataType::getSubcolumnNames` does not enumerate dynamic
-- subcolumns, so the dependency name `j.d` is discovered by scanning the TTL dependencies themselves
-- and resolving each to its name in storage. As with the Tuple subcolumn case, recomputing the
-- part's TTL bounds for a subcolumn dependency is not supported, so the command is refused.
SET allow_experimental_json_type = 1;
DROP TABLE IF EXISTS t_mat_ttl_dynamic_subcolumn;
CREATE TABLE t_mat_ttl_dynamic_subcolumn
    (a Int, j JSON MATERIALIZED CAST(concat('{"d":"', toString(toDateTime(1800000000 + a)), '"}'), 'JSON'))
    ENGINE = MergeTree() ORDER BY a TTL j.d::DateTime + INTERVAL 1 DAY;
ALTER TABLE t_mat_ttl_dynamic_subcolumn MATERIALIZE COLUMN j; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_dynamic_subcolumn;

-- Case 22: A `TTL ... DELETE WHERE <cond>` reads the columns of its WHERE condition (stored in
-- `where_expression_columns`), which `getColumnDependencies` does not expand (it only expands the
-- TTL expression). Materializing a column used only in the WHERE condition would change which rows
-- participate in the rows-where TTL while the mutation copies the part's stale `rows_where_ttl_info`.
-- Recomputing it is not supported (it would require changing the shared dependency expansion, which
-- also drives UPDATE), so — following the same fail-close approach as the subcolumn cases — the
-- command is refused. Here `c3` is only in the WHERE condition (the TTL expression reads `d`).
DROP TABLE IF EXISTS t_mat_ttl_where_full;
CREATE TABLE t_mat_ttl_where_full (a Int, c3 UInt8 MATERIALIZED (a % 2)::UInt8, d DateTime MATERIALIZED toDateTime(1700000000 + a))
    ENGINE = MergeTree() ORDER BY a TTL d + INTERVAL 1 DAY DELETE WHERE c3 = 1;
ALTER TABLE t_mat_ttl_where_full MATERIALIZE COLUMN c3; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_where_full;

-- Case 23: Same as Case 22, but a *subcolumn* of the materialized column is used in the TTL WHERE
-- condition (DELETE WHERE t.k = 1 while materializing the parent Tuple column t). Refused as well.
DROP TABLE IF EXISTS t_mat_ttl_where_subcolumn;
CREATE TABLE t_mat_ttl_where_subcolumn (a Int, t Tuple(k UInt8, v UInt64) MATERIALIZED ((a % 2)::UInt8, a), d DateTime MATERIALIZED toDateTime(1700000000 + a))
    ENGINE = MergeTree() ORDER BY a TTL d + INTERVAL 1 DAY DELETE WHERE t.k = 1;
ALTER TABLE t_mat_ttl_where_subcolumn MATERIALIZE COLUMN t; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_where_subcolumn;

-- Case 24: The WHERE refusal must NOT over-reject when the same materialized column also feeds the
-- TTL *expression*: that already forces a full TTL recalculation (every physical column is fed into
-- the mutation and the whole TTL, including its WHERE condition, is re-evaluated). Materializing `d`,
-- which is in both the TTL expression and its WHERE condition, must therefore still be allowed.
DROP TABLE IF EXISTS t_mat_ttl_where_expr;
CREATE TABLE t_mat_ttl_where_expr (a Int, d DateTime MATERIALIZED toDateTime(1700000000 + a))
    ENGINE = MergeTree() ORDER BY a TTL d + INTERVAL 1 DAY DELETE WHERE d > toDateTime(1700000000);
ALTER TABLE t_mat_ttl_where_expr MATERIALIZE COLUMN d;
DROP TABLE t_mat_ttl_where_expr;

-- Case 25: A skip index over a *subcolumn* of a column that a TTL resets. Materializing `c` drives the
-- column TTL `x TTL c + ...`, which the mutation re-evaluates and can reset `x` (so the stored `x.k`
-- changes); but the minmax index `idx_xk` over `x.k` cannot be rebuilt from the reset parent — the
-- mutation reads the subcolumn `x.k` as an unchanged column straight from the source part rather than
-- deriving it from the recalculated parent, leaving the index with stale bounds (the same gap exists
-- for UPDATE of `c`). Following the same fail-close approach used for subcolumn TTL bounds, refuse.
DROP TABLE IF EXISTS t_mat_ttl_index_subcolumn;
CREATE TABLE t_mat_ttl_index_subcolumn
    (a UInt64, c DateTime MATERIALIZED toDateTime(1000000000),
     x Tuple(k UInt64, v UInt64) TTL c + INTERVAL 1 SECOND,
     INDEX idx_xk x.k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a;
ALTER TABLE t_mat_ttl_index_subcolumn MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_index_subcolumn;

-- Case 26: The Case 25 refusal must NOT over-reject a skip index over the *whole* TTL-target column:
-- that one is rebuilt correctly by the generic derived-object scan (the target column is fully
-- recalculated). `c` is materialized to a past value so the column TTL resets `y` to its default 0;
-- the minmax index `idx_y` over the full column `y` is rebuilt, so a query forced through it for the
-- new value 0 still finds the row (a stale, hardlinked index would prune it away and return 0).
DROP TABLE IF EXISTS t_mat_ttl_index_full;
CREATE TABLE t_mat_ttl_index_full
    (a UInt64, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     INDEX idx_y y TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_mat_ttl_index_full (a, y) VALUES (1, 100);
ALTER TABLE t_mat_ttl_index_full MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_index_full MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT count() FROM t_mat_ttl_index_full WHERE y = 0 SETTINGS force_data_skipping_indices = 'idx_y';
DROP TABLE t_mat_ttl_index_full;
