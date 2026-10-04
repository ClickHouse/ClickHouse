-- Materializing a column must be refused when it would leave derived data stale, and must
-- recompute the dependent data otherwise (continuation of `04046_materialize_column_sort_key_expr`).
-- See https://github.com/ClickHouse/ClickHouse/issues/93139

-- Case 17: The CollapsingMergeTree sign column is a merge-semantic key column even when it is not in
-- ORDER BY. UPDATE of it is refused via getKeyColumns; MATERIALIZE COLUMN rewrites it just the same,
-- so it must be refused too — otherwise the collapsing semantics of existing data would be corrupted.
DROP TABLE IF EXISTS t_mat_sign;
CREATE TABLE t_mat_sign (a Int, sign Int8 MATERIALIZED 1) ENGINE = CollapsingMergeTree(sign) ORDER BY a;
INSERT INTO t_mat_sign (a) VALUES (1);
ALTER TABLE t_mat_sign MATERIALIZE COLUMN sign; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_sign;

-- Case 18: Same for the ReplacingMergeTree version column.
DROP TABLE IF EXISTS t_mat_version;
CREATE TABLE t_mat_version (a Int, ver UInt32 MATERIALIZED 1) ENGINE = ReplacingMergeTree(ver) ORDER BY a;
INSERT INTO t_mat_version (a) VALUES (1);
ALTER TABLE t_mat_version MATERIALIZE COLUMN ver; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_version;

-- Case 19: A stored MATERIALIZED column computed from the materialized column is itself the engine
-- sign column. Recomputing the source column would recompute the sign, so it must be refused for the
-- same reason as the direct sign-column case.
DROP TABLE IF EXISTS t_mat_dep_sign;
CREATE TABLE t_mat_dep_sign (a Int, c2 Int MATERIALIZED a, s Int8 MATERIALIZED c2) ENGINE = CollapsingMergeTree(s) ORDER BY a;
INSERT INTO t_mat_dep_sign (a) VALUES (1);
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
INSERT INTO t_mat_ttl_subcolumn (a) VALUES (1);
ALTER TABLE t_mat_ttl_subcolumn MATERIALIZE COLUMN t; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_subcolumn;

-- Case 21: Same as Case 20, but the TTL reads a *dynamic* subcolumn — a JSON path (TTL j.d while
-- materializing the parent JSON column j). `IDataType::getSubcolumnNames` does not enumerate dynamic
-- subcolumns, so the dependency name `j.d` is discovered by scanning the TTL dependencies themselves
-- and resolving each to its name in storage. As with the Tuple subcolumn case, recomputing the
-- part's TTL bounds for a subcolumn dependency is not supported, so the command is refused.
SET allow_experimental_json_type = 1;
-- `toDateTime` over the Dynamic subcolumn `j.d` is now rejected as suspicious at CREATE
-- (it cannot handle every type a Dynamic column can store). The point of this case is the
-- dynamic-subcolumn TTL *dependency*, not the TTL expression itself, so allow it explicitly.
SET allow_suspicious_ttl_expressions = 1;
DROP TABLE IF EXISTS t_mat_ttl_dynamic_subcolumn;
CREATE TABLE t_mat_ttl_dynamic_subcolumn
    (a Int, j JSON MATERIALIZED CAST(concat('{"d":"', toString(toDateTime(1800000000 + a)), '"}'), 'JSON'))
    ENGINE = MergeTree() ORDER BY a TTL j.d::DateTime + INTERVAL 1 DAY;
INSERT INTO t_mat_ttl_dynamic_subcolumn (a) VALUES (1);
ALTER TABLE t_mat_ttl_dynamic_subcolumn MATERIALIZE COLUMN j; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_dynamic_subcolumn;
SET allow_suspicious_ttl_expressions = 0;

-- Case 22: A `TTL ... DELETE WHERE <cond>` reads the columns of its WHERE condition (stored in
-- `where_expression_columns`). `getColumnDependencies` expands them for a rows-where TTL the same
-- way as the TTL expression columns, so materializing a column used only in the WHERE condition
-- forces a full TTL recalculation that re-evaluates the WHERE with the recomputed values — the
-- command is allowed and must not be refused. Here `c3` is only in the WHERE condition (the TTL
-- expression reads `d`, which is already expired): after `c3` is rematerialized to `(a % 2)`, the
-- row with `a = 1` matches the DELETE WHERE and is removed by the TTL pass of the mutation, while
-- `a = 2` stays (a stale, hardlinked rows-where TTL decision would keep both rows).
DROP TABLE IF EXISTS t_mat_ttl_where_full;
CREATE TABLE t_mat_ttl_where_full (a Int, c3 UInt8 MATERIALIZED 0, d DateTime MATERIALIZED toDateTime(1000000000))
    ENGINE = MergeTree() ORDER BY a TTL d + INTERVAL 1 SECOND DELETE WHERE c3 = 1;
INSERT INTO t_mat_ttl_where_full (a) VALUES (1), (2);
ALTER TABLE t_mat_ttl_where_full MODIFY COLUMN c3 UInt8 MATERIALIZED (a % 2)::UInt8;
ALTER TABLE t_mat_ttl_where_full MATERIALIZE COLUMN c3 SETTINGS mutations_sync = 2;
SELECT a FROM t_mat_ttl_where_full;
DROP TABLE t_mat_ttl_where_full;

-- Case 23: Same as Case 22, but a *subcolumn* of the materialized column is used in the TTL WHERE
-- condition (DELETE WHERE t.k = 1 while materializing the parent Tuple column t). Refused as well.
DROP TABLE IF EXISTS t_mat_ttl_where_subcolumn;
CREATE TABLE t_mat_ttl_where_subcolumn (a Int, t Tuple(k UInt8, v UInt64) MATERIALIZED ((a % 2)::UInt8, a), d DateTime MATERIALIZED toDateTime(1700000000 + a))
    ENGINE = MergeTree() ORDER BY a TTL d + INTERVAL 1 DAY DELETE WHERE t.k = 1;
INSERT INTO t_mat_ttl_where_subcolumn (a) VALUES (1);
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
INSERT INTO t_mat_ttl_index_subcolumn (a) VALUES (1);
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

-- Case 27: A column used only in a rows-where TTL WHERE condition is allowed even when a separate
-- *column* TTL also produces a TTL_TARGET dependency for the materialized column. Materializing `c`
-- feeds the column TTL `x TTL c + INTERVAL 1 SECOND` (so `c` yields a TTL_TARGET for `x`) and the
-- rows-where WHERE `c > ...`; the WHERE dependency is expanded by `getColumnDependencies` like the
-- TTL expression columns, so the rows-where TTL is re-evaluated with the recomputed `c` and nothing
-- is left stale.
DROP TABLE IF EXISTS t_mat_ttl_where_column_ttl;
CREATE TABLE t_mat_ttl_where_column_ttl
    (a Int,
     c DateTime MATERIALIZED toDateTime(1700000000 + a),
     d DateTime MATERIALIZED toDateTime(1700000000 + a),
     x UInt64 TTL c + INTERVAL 1 SECOND)
    ENGINE = MergeTree() ORDER BY a TTL d + INTERVAL 1 DAY DELETE WHERE c > toDateTime(1500000000);
INSERT INTO t_mat_ttl_where_column_ttl (a) VALUES (1);
ALTER TABLE t_mat_ttl_where_column_ttl MATERIALIZE COLUMN c;
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
