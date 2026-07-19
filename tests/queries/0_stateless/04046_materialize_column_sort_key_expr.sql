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

-- Case 27: A column used only in a rows-where TTL WHERE condition must be refused even when a separate
-- *column* TTL produces a TTL_TARGET dependency for the materialized column. Materializing `c` feeds the
-- column TTL `x TTL c + INTERVAL 1 SECOND` (so `c` yields a TTL_TARGET for `x`), but a column-TTL target
-- does NOT re-evaluate the rows-where TTL `DELETE WHERE c > ...` — the part's rows-where TTL bounds would
-- be left stale. The full-TTL-recalculation shortcut must therefore key off the row/group TTL
-- *expression* columns (here `d`, which is unchanged), not any TTL_TARGET, so the command is refused.
DROP TABLE IF EXISTS t_mat_ttl_where_column_ttl;
CREATE TABLE t_mat_ttl_where_column_ttl
    (a Int,
     c DateTime MATERIALIZED toDateTime(1700000000 + a),
     d DateTime MATERIALIZED toDateTime(1700000000 + a),
     x UInt64 TTL c + INTERVAL 1 SECOND)
    ENGINE = MergeTree() ORDER BY a TTL d + INTERVAL 1 DAY DELETE WHERE c > toDateTime(1500000000);
ALTER TABLE t_mat_ttl_where_column_ttl MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_where_column_ttl;

-- Case 28: The precise `full_ttl_recalc` must NOT over-reject when the materialized column feeds the
-- rows-where TTL *expression* directly (which does force a full TTL recalculation that re-evaluates the
-- WHERE too), even if it also drives a column TTL. Materializing `c`, used in the rows-where TTL
-- expression `c + INTERVAL 1 DAY` (and the column TTL `x TTL c + ...`) and in its WHERE condition,
-- must still be allowed.
DROP TABLE IF EXISTS t_mat_ttl_expr_with_column_ttl;
CREATE TABLE t_mat_ttl_expr_with_column_ttl
    (a Int,
     c DateTime MATERIALIZED toDateTime(1700000000 + a),
     x UInt64 TTL c + INTERVAL 1 SECOND)
    ENGINE = MergeTree() ORDER BY a TTL c + INTERVAL 1 DAY DELETE WHERE c > toDateTime(1500000000);
ALTER TABLE t_mat_ttl_expr_with_column_ttl MATERIALIZE COLUMN c;
DROP TABLE t_mat_ttl_expr_with_column_ttl;

-- Case 29: A skip index that reads a TTL-target column inside a *computed expression* together with a
-- sibling column (`INDEX idx (a + y)`) while a column TTL resets `y`. Materializing `c` drives the
-- column TTL `y TTL c + ...`, which resets `y`; the generic derived-object scan marks the index for
-- rebuild, but the mutation recomputes the index expression from a block where `y` still holds its
-- pre-reset value (it is read as an unchanged column from the source part rather than the recalculated
-- one), leaving the index stale — a query forced through it for `a + y = 5` would be pruned. Unlike a
-- plain-column index over the same target (Case 26), this shape cannot be rebuilt correctly by the
-- shared mutation machinery (UPDATE of `c` leaves it equally stale), so it is refused.
DROP TABLE IF EXISTS t_mat_ttl_index_expr;
CREATE TABLE t_mat_ttl_index_expr
    (a UInt64, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     INDEX idx_ay (a + y) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1;
ALTER TABLE t_mat_ttl_index_expr MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_index_expr;

-- Case 30: Same limitation with a single-column computed expression (`INDEX idx (y + 1)`, no sibling) —
-- confirming the refusal is about the computed expression over the reset target, not a missing sibling
-- column. Also refused.
DROP TABLE IF EXISTS t_mat_ttl_index_expr_single;
CREATE TABLE t_mat_ttl_index_expr_single
    (a UInt64, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     INDEX idx_ye (y + 1) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1;
ALTER TABLE t_mat_ttl_index_expr_single MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_index_expr_single;

-- Case 31: The Case 29/30 refusals must NOT over-reject a *plain-column* skip index that reads the
-- TTL-target column together with a sibling column as separate top-level index expressions
-- (`INDEX idx (a, y)`, a per-column minmax). Each element is a bare column, so it is rebuilt correctly
-- (the target `y` is recalculated and its plain minmax derived from it): after the TTL resets `y` to 0
-- the row still has `y = 0`, and a query forced through the index finds it (a stale index would prune it).
DROP TABLE IF EXISTS t_mat_ttl_index_plain_multi;
CREATE TABLE t_mat_ttl_index_plain_multi
    (a UInt64, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     INDEX idx_a_y (a, y) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_mat_ttl_index_plain_multi (a, y) VALUES (5, 100);
ALTER TABLE t_mat_ttl_index_plain_multi MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_index_plain_multi MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT count() FROM t_mat_ttl_index_plain_multi WHERE y = 0 SETTINGS force_data_skipping_indices = 'idx_a_y';
DROP TABLE t_mat_ttl_index_plain_multi;

-- Case 32: A projection that reads a TTL-target column together with a *sibling* column
-- (`PROJECTION p (SELECT a, y ORDER BY a)`) while a column TTL resets `y`. Materializing `c` drives
-- the column TTL `y TTL c + ...` (so `y` lands in the mutation's changed columns and the projection is
-- marked for rebuild), but the sibling `a` is neither the materialized column nor a TTL dependency, so
-- it must be fed into the mutation stream too. On a *wide* part the rebuild otherwise fails with
-- `NOT_FOUND_COLUMN_IN_BLOCK` for `a`. After the fix the command succeeds and the projection is rebuilt
-- from the reset values: the forced projection returns the reset `y = 0` (not the stale 100), and `a` is
-- preserved. Forces a wide part so the sibling actually has to be fed (a compact part carries all
-- columns anyway).
DROP TABLE IF EXISTS t_mat_ttl_proj_sibling;
CREATE TABLE t_mat_ttl_proj_sibling
    (a UInt64, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     PROJECTION p (SELECT a, y ORDER BY a))
    ENGINE = MergeTree() ORDER BY a SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_ttl_proj_sibling (a, y) VALUES (5, 100);
ALTER TABLE t_mat_ttl_proj_sibling MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_proj_sibling MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT a, y FROM t_mat_ttl_proj_sibling ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_mat_ttl_proj_sibling;

-- Case 33: The same sibling-feeding is required for a plain-column skip index over a TTL-target column
-- on a wide part (`INDEX idx (a, y)` while a column TTL resets `y`). This is the wide-part counterpart of
-- Case 31 (which uses a compact part and so never exercises the missing sibling): without feeding `a`
-- the rebuild fails with `UNKNOWN_IDENTIFIER` for `a`. After the fix the command succeeds and the index
-- is rebuilt from the reset value, so a query forced through it for `y = 0` still finds the row.
DROP TABLE IF EXISTS t_mat_ttl_index_plain_multi_wide;
CREATE TABLE t_mat_ttl_index_plain_multi_wide
    (a UInt64, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     INDEX idx_a_y (a, y) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_ttl_index_plain_multi_wide (a, y) VALUES (5, 100);
ALTER TABLE t_mat_ttl_index_plain_multi_wide MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_index_plain_multi_wide MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT count() FROM t_mat_ttl_index_plain_multi_wide WHERE y = 0 SETTINGS force_data_skipping_indices = 'idx_a_y';
DROP TABLE t_mat_ttl_index_plain_multi_wide;

-- Case 34: The sibling-feeding must also produce a *correct* aggregate projection over a wide part, not
-- merely avoid the exception. `PROJECTION p (SELECT a, sum(y) GROUP BY a)` reads the TTL-target `y` and
-- the sibling `a`; after the reset the forced projection returns `sum(y) = 0` per group (a stale
-- projection would report the pre-reset 300), confirming the projection is rebuilt from the reset block.
DROP TABLE IF EXISTS t_mat_ttl_proj_agg_sibling;
CREATE TABLE t_mat_ttl_proj_agg_sibling
    (a UInt64, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     PROJECTION p (SELECT a, sum(y) GROUP BY a))
    ENGINE = MergeTree() ORDER BY a SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_ttl_proj_agg_sibling (a, y) VALUES (1, 100) (1, 200) (2, 300);
ALTER TABLE t_mat_ttl_proj_agg_sibling MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_proj_agg_sibling MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT a, sum(y) FROM t_mat_ttl_proj_agg_sibling GROUP BY a ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_mat_ttl_proj_agg_sibling;

-- Case 35: The ReplacingMergeTree is_deleted column is a merge-semantic key column just like the sign
-- and version columns (Cases 17/18): merge / FINAL winner selection and cleanup depend on it, so
-- recomputing it could flip the delete markers of existing rows. Refused.
DROP TABLE IF EXISTS t_mat_is_deleted;
CREATE TABLE t_mat_is_deleted (a Int, ver UInt32, d UInt8 MATERIALIZED 0)
    ENGINE = ReplacingMergeTree(ver, d) ORDER BY a;
ALTER TABLE t_mat_is_deleted MATERIALIZE COLUMN d; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_is_deleted;

-- Case 36: A stored MATERIALIZED column computed from the materialized column is itself the engine
-- is_deleted column — the indirect counterpart of Case 35, mirroring Case 19 for the sign column.
DROP TABLE IF EXISTS t_mat_dep_is_deleted;
CREATE TABLE t_mat_dep_is_deleted (a Int, ver UInt32, c2 UInt8 MATERIALIZED 0, d UInt8 MATERIALIZED c2)
    ENGINE = ReplacingMergeTree(ver, d) ORDER BY a;
ALTER TABLE t_mat_dep_is_deleted MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_dep_is_deleted;

-- Case 37: A column TTL can target a merge-semantic column (`checkTTLExpressions` forbids a column TTL
-- only on sorting / partition key columns), so materializing `c` with `sign Int8 TTL c + INTERVAL ...`
-- would reset the sign column through the TTL side effect after the direct (Case 17) and dependent
-- (Case 19) checks have already passed. Refused up front.
DROP TABLE IF EXISTS t_mat_ttl_sign;
CREATE TABLE t_mat_ttl_sign
    (a Int, c DateTime MATERIALIZED toDateTime(2000000000), sign Int8 TTL c + INTERVAL 1 SECOND)
    ENGINE = CollapsingMergeTree(sign) ORDER BY a;
ALTER TABLE t_mat_ttl_sign MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_sign;

-- Case 38: Same as Case 37, with the is_deleted column as the TTL target.
DROP TABLE IF EXISTS t_mat_ttl_is_deleted;
CREATE TABLE t_mat_ttl_is_deleted
    (a Int, ver UInt32, c DateTime MATERIALIZED toDateTime(2000000000), d UInt8 TTL c + INTERVAL 1 SECOND)
    ENGINE = ReplacingMergeTree(ver, d) ORDER BY a;
ALTER TABLE t_mat_ttl_is_deleted MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_is_deleted;

-- Case 39: A skip index reading a *subcolumn* of the materialized column itself (`INDEX idx t.k`
-- while materializing the parent Tuple column `t`). The readonly recalculation stage reads such a
-- subcolumn dependency straight from the source part (it is never rewritten through `getSubcolumn`
-- of the recomputed parent), so the index would be rebuilt from the pre-rewrite values and a query
-- forced through it could be pruned incorrectly. Refused, same as the subcolumn TTL dependencies.
DROP TABLE IF EXISTS t_mat_index_subcolumn;
CREATE TABLE t_mat_index_subcolumn
    (a UInt64, t Tuple(k UInt64) MATERIALIZED tuple(a * 10),
     INDEX idx_tk t.k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_index_subcolumn (a) VALUES (5);
ALTER TABLE t_mat_index_subcolumn MODIFY COLUMN t Tuple(k UInt64) MATERIALIZED tuple(a * 10 + 1000);
ALTER TABLE t_mat_index_subcolumn MATERIALIZE COLUMN t; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_index_subcolumn;

-- Case 40: Same through a dependent stored MATERIALIZED column — the recomputed set includes the
-- dependent parent `t`, whose subcolumn is read by the index. Refused.
DROP TABLE IF EXISTS t_mat_dep_index_subcolumn;
CREATE TABLE t_mat_dep_index_subcolumn
    (a UInt64, c2 UInt64 MATERIALIZED a * 10, t Tuple(k UInt64) MATERIALIZED tuple(c2 + 1),
     INDEX idx_tk t.k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_dep_index_subcolumn (a) VALUES (5);
ALTER TABLE t_mat_dep_index_subcolumn MODIFY COLUMN c2 UInt64 MATERIALIZED a * 10 + 1000;
ALTER TABLE t_mat_dep_index_subcolumn MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_dep_index_subcolumn;

-- Case 41: No over-rejection — a skip index on a subcolumn of an *unrelated* column must not block
-- the command, and the index still prunes correctly afterwards.
DROP TABLE IF EXISTS t_mat_index_subcolumn_unrelated;
CREATE TABLE t_mat_index_subcolumn_unrelated
    (a UInt64, c2 UInt64 MATERIALIZED a * 10, u Tuple(k UInt64),
     INDEX idx_uk u.k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_index_subcolumn_unrelated (a, u) VALUES (5, tuple(7));
ALTER TABLE t_mat_index_subcolumn_unrelated MODIFY COLUMN c2 UInt64 MATERIALIZED a * 10 + 1000;
ALTER TABLE t_mat_index_subcolumn_unrelated MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT c2, u.k FROM t_mat_index_subcolumn_unrelated WHERE u.k = 7 SETTINGS force_data_skipping_indices = 'idx_uk';
DROP TABLE t_mat_index_subcolumn_unrelated;
