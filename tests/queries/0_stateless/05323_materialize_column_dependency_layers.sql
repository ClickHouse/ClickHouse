-- Materializing a column must be refused when it would leave derived data stale, and must
-- recompute the dependent data otherwise (continuation of `04046_materialize_column_sort_key_expr`).
-- See https://github.com/ClickHouse/ClickHouse/issues/93139

-- Case 42: Two *directly* affected MATERIALIZED columns where one reads the other
-- (`m MATERIALIZED c2 + 1, n MATERIALIZED c2 + m`). All expressions of one mutation stage are
-- resolved against the block as it was before the stage, so recomputing `m` and `n` together would
-- evaluate `n` over the old stored `m`. The dependent columns are split into dependency layers, so
-- `n` must see the recomputed `m`.
DROP TABLE IF EXISTS t_mat_dep_sibling;
CREATE TABLE t_mat_dep_sibling
    (a UInt64, c2 UInt64 MATERIALIZED a * 10, m UInt64 MATERIALIZED c2 + 1, n UInt64 MATERIALIZED c2 + m)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_dep_sibling (a) VALUES (5);
SELECT c2, m, n FROM t_mat_dep_sibling;
ALTER TABLE t_mat_dep_sibling MODIFY COLUMN c2 UInt64 MATERIALIZED a * 10 + 1000;
ALTER TABLE t_mat_dep_sibling MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT c2, m, n FROM t_mat_dep_sibling;
DROP TABLE t_mat_dep_sibling;

-- Case 43: A chain of three directly affected MATERIALIZED columns, each reading the previous one
-- (three dependency layers). Every column must be recomputed from the already-recomputed ones.
DROP TABLE IF EXISTS t_mat_dep_sibling_chain;
CREATE TABLE t_mat_dep_sibling_chain
    (a UInt64, c2 UInt64 MATERIALIZED a * 10, m UInt64 MATERIALIZED c2 + 1,
     n UInt64 MATERIALIZED c2 + m, o UInt64 MATERIALIZED c2 + n)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_dep_sibling_chain (a) VALUES (5);
SELECT c2, m, n, o FROM t_mat_dep_sibling_chain;
ALTER TABLE t_mat_dep_sibling_chain MODIFY COLUMN c2 UInt64 MATERIALIZED a * 10 + 1000;
ALTER TABLE t_mat_dep_sibling_chain MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT c2, m, n, o FROM t_mat_dep_sibling_chain;
DROP TABLE t_mat_dep_sibling_chain;

-- Case 44: A skip index over the dependent column that reads a recomputed sibling must be rebuilt
-- from the correct value — with a stale `n` the forced index would prune the granule away.
DROP TABLE IF EXISTS t_mat_dep_sibling_index;
CREATE TABLE t_mat_dep_sibling_index
    (a UInt64, c2 UInt64 MATERIALIZED a * 10, m UInt64 MATERIALIZED c2 + 1, n UInt64 MATERIALIZED c2 + m,
     INDEX idx_n n TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_dep_sibling_index (a) VALUES (5);
ALTER TABLE t_mat_dep_sibling_index MODIFY COLUMN c2 UInt64 MATERIALIZED a * 10 + 1000;
ALTER TABLE t_mat_dep_sibling_index MATERIALIZE COLUMN c2 SETTINGS mutations_sync = 2;
SELECT n FROM t_mat_dep_sibling_index WHERE n = 2101 SETTINGS force_data_skipping_indices = 'idx_n';
DROP TABLE t_mat_dep_sibling_index;

-- Case 45: A mixed mutation `UPDATE parent, MATERIALIZE COLUMN m` where the materialized
-- expression reads a *subcolumn* of the updated parent (`m MATERIALIZED t.k + 1`). Without
-- rewriting the subcolumn to `getSubcolumn` of the recomputed parent, `t.k` is registered as a
-- separate stage input read from the source part, so `m` would be computed from the pre-update
-- subcolumn values. Both wide and compact parts.
DROP TABLE IF EXISTS t_mat_mixed_subcolumn_wide;
CREATE TABLE t_mat_mixed_subcolumn_wide
    (a UInt64, t Tuple(k UInt64, v UInt64), m UInt64 MATERIALIZED t.k + 1)
    ENGINE = MergeTree() ORDER BY a SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mat_mixed_subcolumn_wide (a, t) VALUES (1, (10, 100));
SELECT t, m FROM t_mat_mixed_subcolumn_wide;
ALTER TABLE t_mat_mixed_subcolumn_wide UPDATE t = tuple(20, 200) WHERE 1, MATERIALIZE COLUMN m SETTINGS mutations_sync = 2;
SELECT t, m FROM t_mat_mixed_subcolumn_wide;
DROP TABLE t_mat_mixed_subcolumn_wide;

DROP TABLE IF EXISTS t_mat_mixed_subcolumn_compact;
CREATE TABLE t_mat_mixed_subcolumn_compact
    (a UInt64, t Tuple(k UInt64, v UInt64), m UInt64 MATERIALIZED t.k + 1)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_mixed_subcolumn_compact (a, t) VALUES (1, (10, 100));
ALTER TABLE t_mat_mixed_subcolumn_compact UPDATE t = tuple(20, 200) WHERE 1, MATERIALIZE COLUMN m SETTINGS mutations_sync = 2;
SELECT t, m FROM t_mat_mixed_subcolumn_compact;
DROP TABLE t_mat_mixed_subcolumn_compact;

-- Case 46: The refusals are metadata-based and run at validation time, before any part is selected,
-- so they do not depend on whether the existing parts already carry the derived object. A projection
-- added after the data was inserted still refuses materializing a column used by its sorting key.
DROP TABLE IF EXISTS t_mat_projection_added_late;
CREATE TABLE t_mat_projection_added_late
    (a UInt64, c2 UInt64 MATERIALIZED a * 10)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_projection_added_late (a) VALUES (5);
ALTER TABLE t_mat_projection_added_late ADD PROJECTION p (SELECT * ORDER BY metroHash64(c2));
ALTER TABLE t_mat_projection_added_late MODIFY COLUMN c2 UInt64 MATERIALIZED a * 10 + 1000;
ALTER TABLE t_mat_projection_added_late MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_projection_added_late;

-- Case 47: Likewise for a subcolumn skip index added after the data was inserted: the command is
-- refused up front rather than accepted and then failed while processing the part.
DROP TABLE IF EXISTS t_mat_index_added_late;
CREATE TABLE t_mat_index_added_late
    (a UInt64, t Tuple(k UInt64) MATERIALIZED tuple(a * 10))
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_index_added_late (a) VALUES (5);
ALTER TABLE t_mat_index_added_late ADD INDEX idx_tk t.k TYPE minmax GRANULARITY 1;
ALTER TABLE t_mat_index_added_late MODIFY COLUMN t Tuple(k UInt64) MATERIALIZED tuple(a * 10 + 1000);
ALTER TABLE t_mat_index_added_late MATERIALIZE COLUMN t; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_index_added_late;

-- Case 48: A TTL-driven reset must not leave a stored MATERIALIZED column stale. Materializing `c`
-- makes the mutation run the full TTL pass, which re-evaluates the column TTL `y TTL c + ...` and
-- resets `y`; but `m MATERIALIZED y + 1` never enters the recompute machinery (`y` is neither
-- updated nor materialized), and the recompute stages run before the TTL transform anyway, so the
-- new part would hold fresh `y` and stale `m` (and any skip index / projection / statistics over
-- `m` would stay stale with it). Refused up front.
DROP TABLE IF EXISTS t_mat_ttl_target_materialized;
CREATE TABLE t_mat_ttl_target_materialized
    (a UInt64, c DateTime MATERIALIZED toDateTime(1000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     m UInt64 MATERIALIZED y + 1)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_ttl_target_materialized (a, y) VALUES (1, 100);
ALTER TABLE t_mat_ttl_target_materialized MATERIALIZE COLUMN c; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE t_mat_ttl_target_materialized;

-- Case 49: The Case 48 refusal must NOT over-reject a stored MATERIALIZED column that does not read
-- the TTL target: `m MATERIALIZED a + 1` is untouched by the reset of `y`, so the command is allowed;
-- the TTL pass resets `y` to its default 0 while `m` keeps its consistent value.
DROP TABLE IF EXISTS t_mat_ttl_target_unrelated_mat;
CREATE TABLE t_mat_ttl_target_unrelated_mat
    (a UInt64, c DateTime MATERIALIZED toDateTime(1000000000),
     y UInt64 TTL c + INTERVAL 1 SECOND,
     m UInt64 MATERIALIZED a + 1)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_mat_ttl_target_unrelated_mat (a, y) VALUES (1, 100);
ALTER TABLE t_mat_ttl_target_unrelated_mat MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT y, m FROM t_mat_ttl_target_unrelated_mat;
DROP TABLE t_mat_ttl_target_unrelated_mat;
