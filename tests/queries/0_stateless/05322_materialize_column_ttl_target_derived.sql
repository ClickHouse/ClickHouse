-- Materializing a column must be refused when it would leave derived data stale, and must
-- recompute the dependent data otherwise (continuation of `04046_materialize_column_sort_key_expr`).
-- See https://github.com/ClickHouse/ClickHouse/issues/93139

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
INSERT INTO t_mat_ttl_index_expr (a, y) VALUES (1, 1);
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
INSERT INTO t_mat_ttl_index_expr_single (a, y) VALUES (1, 1);
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
    (a UInt64, b String, c DateTime MATERIALIZED toDateTime(2000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     PROJECTION p (SELECT a, y ORDER BY a))
    ENGINE = MergeTree() ORDER BY b SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_ttl_proj_sibling (a, b, y) VALUES (5, 'x', 100);
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
INSERT INTO t_mat_is_deleted (a, ver) VALUES (1, 1);
ALTER TABLE t_mat_is_deleted MATERIALIZE COLUMN d; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_is_deleted;

-- Case 36: A stored MATERIALIZED column computed from the materialized column is itself the engine
-- is_deleted column — the indirect counterpart of Case 35, mirroring Case 19 for the sign column.
DROP TABLE IF EXISTS t_mat_dep_is_deleted;
CREATE TABLE t_mat_dep_is_deleted (a Int, ver UInt32, c2 UInt8 MATERIALIZED 0, d UInt8 MATERIALIZED c2)
    ENGINE = ReplacingMergeTree(ver, d) ORDER BY a;
INSERT INTO t_mat_dep_is_deleted (a, ver) VALUES (1, 1);
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
INSERT INTO t_mat_ttl_sign (a, sign) VALUES (1, 1);
ALTER TABLE t_mat_ttl_sign MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_sign;

-- Case 38: Same as Case 37, with the is_deleted column as the TTL target.
DROP TABLE IF EXISTS t_mat_ttl_is_deleted;
CREATE TABLE t_mat_ttl_is_deleted
    (a Int, ver UInt32, c DateTime MATERIALIZED toDateTime(2000000000), d UInt8 TTL c + INTERVAL 1 SECOND)
    ENGINE = ReplacingMergeTree(ver, d) ORDER BY a;
INSERT INTO t_mat_ttl_is_deleted (a, ver) VALUES (1, 1);
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
