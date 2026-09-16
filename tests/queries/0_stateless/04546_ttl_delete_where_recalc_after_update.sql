DROP TABLE IF EXISTS ttl_delete_where_recalc;

CREATE TABLE ttl_delete_where_recalc
(
    d DateTime,
    should_delete UInt8 DEFAULT 0,
    val UInt32
)
ENGINE = MergeTree
ORDER BY tuple()
TTL d + INTERVAL 1 SECOND DELETE WHERE should_delete = 1
SETTINGS min_bytes_for_wide_part = 0;

-- All rows are time-expired (d far in the past) but not yet flagged, so DELETE WHERE does not match.
INSERT INTO ttl_delete_where_recalc (d, should_delete, val) VALUES ('2000-01-01 00:00:00', 0, 1), ('2000-01-01 00:00:00', 0, 2), ('2000-01-01 00:00:00', 0, 3);

SELECT count() FROM ttl_delete_where_recalc;

-- Flip the WHERE-referenced flag on two rows via a mutation. They now satisfy DELETE WHERE and are expired.
ALTER TABLE ttl_delete_where_recalc UPDATE should_delete = 1 WHERE val <= 2 SETTINGS mutations_sync = 2;

-- The mutation must recalculate rows_where_ttl_info, so the two flagged rows are dropped immediately.
SELECT count() FROM ttl_delete_where_recalc;

-- A follow-up OPTIMIZE FINAL must not resurrect them and must keep the unflagged row.
OPTIMIZE TABLE ttl_delete_where_recalc FINAL;
SELECT count() FROM ttl_delete_where_recalc;
SELECT val FROM ttl_delete_where_recalc ORDER BY val;

DROP TABLE ttl_delete_where_recalc;

-- The WHERE-referenced column may be a MATERIALIZED column. Updating its source column
-- must recalculate the MATERIALIZED value and, transitively, the DELETE WHERE TTL info.
DROP TABLE IF EXISTS ttl_delete_where_materialized;

CREATE TABLE ttl_delete_where_materialized
(
    d DateTime,
    src UInt8,
    flag UInt8 MATERIALIZED src,
    id UInt8
)
ENGINE = MergeTree
ORDER BY tuple()
TTL d + INTERVAL 1 SECOND DELETE WHERE flag = 1
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_delete_where_materialized (d, src, id) VALUES ('2000-01-01 00:00:00', 0, 1);

SELECT count() FROM ttl_delete_where_materialized;

-- Updating src recomputes flag (MATERIALIZED). The row now matches DELETE WHERE and is expired.
ALTER TABLE ttl_delete_where_materialized UPDATE src = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE ttl_delete_where_materialized FINAL;
SELECT count() FROM ttl_delete_where_materialized;

DROP TABLE ttl_delete_where_materialized;

-- The DELETE WHERE column may be reached only through a chain of MATERIALIZED columns
-- (m3 MATERIALIZED m2 MATERIALIZED m1 MATERIALIZED src). Updating the base column must
-- recompute the whole chain and, transitively, the DELETE WHERE TTL info.
DROP TABLE IF EXISTS ttl_delete_where_materialized_chain;

CREATE TABLE ttl_delete_where_materialized_chain
(
    d DateTime,
    src UInt8,
    m1 UInt8 MATERIALIZED src,
    m2 UInt8 MATERIALIZED m1,
    m3 UInt8 MATERIALIZED m2,
    id UInt8
)
ENGINE = MergeTree
ORDER BY tuple()
TTL d + INTERVAL 1 SECOND DELETE WHERE m3 = 1
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_delete_where_materialized_chain (d, src, id) VALUES ('2000-01-01 00:00:00', 0, 1);

SELECT count() FROM ttl_delete_where_materialized_chain;

-- Updating src must recompute m1 -> m2 -> m3, each reading the freshly recomputed predecessor.
ALTER TABLE ttl_delete_where_materialized_chain UPDATE src = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE ttl_delete_where_materialized_chain FINAL;
SELECT count() FROM ttl_delete_where_materialized_chain;

DROP TABLE ttl_delete_where_materialized_chain;

-- CLEAR COLUMN of a base column must recompute a chain of MATERIALIZED columns in
-- dependency order. Here m2 MATERIALIZED m1 MATERIALIZED src: after CLEAR COLUMN src
-- the stored m2 must be rebuilt from the freshly recomputed m1, not the pre-clear m1.
DROP TABLE IF EXISTS clear_column_materialized_chain;

CREATE TABLE clear_column_materialized_chain
(
    src UInt8,
    m1 UInt8 MATERIALIZED src + 0,
    m2 UInt8 MATERIALIZED m1 + 0,
    id UInt8
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO clear_column_materialized_chain (src, id) VALUES (5, 1);

-- Materialize m1, m2 on disk (both become 5).
ALTER TABLE clear_column_materialized_chain MATERIALIZE COLUMN m1 SETTINGS mutations_sync = 2;
ALTER TABLE clear_column_materialized_chain MATERIALIZE COLUMN m2 SETTINGS mutations_sync = 2;

SELECT src, m1, m2 FROM clear_column_materialized_chain;

-- Clearing src resets it to 0; m1 -> 0 and m2 must recompute from the new m1 -> 0.
ALTER TABLE clear_column_materialized_chain CLEAR COLUMN src SETTINGS mutations_sync = 2;

SELECT src, m1, m2 FROM clear_column_materialized_chain;

DROP TABLE clear_column_materialized_chain;

-- A patch part created before a MATERIALIZED chain was grown carries only the earlier
-- hops (here {src, m1}). ALTER TABLE ... APPLY PATCHES must expand transitively through
-- the chain and recompute m2/m3 (and recalculate the DELETE WHERE TTL that references
-- m3) instead of relying on whatever the old-shape patch stored.
DROP TABLE IF EXISTS apply_patches_materialized_chain;

CREATE TABLE apply_patches_materialized_chain
(
    d DateTime,
    src UInt8,
    m1 UInt8 MATERIALIZED src,
    id UInt8
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         enable_block_number_column = 1,
         enable_block_offset_column = 1,
         apply_patches_on_merge = 0;

INSERT INTO apply_patches_materialized_chain (d, src, id) VALUES ('2000-01-01 00:00:00', 0, 1);

-- Lightweight update creates a patch part that carries only src and m1.
UPDATE apply_patches_materialized_chain SET src = 1 WHERE id = 1
SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

-- Grow the chain and add the DELETE WHERE TTL on the deepest hop. Do not let MODIFY TTL
-- itself recalculate the TTL info, so APPLY PATCHES is the only thing that can.
ALTER TABLE apply_patches_materialized_chain
    ADD COLUMN m2 UInt8 MATERIALIZED m1,
    ADD COLUMN m3 UInt8 MATERIALIZED m2 SETTINGS mutations_sync = 2;
ALTER TABLE apply_patches_materialized_chain
    MODIFY TTL d + INTERVAL 1 SECOND DELETE WHERE m3 = 1
    SETTINGS materialize_ttl_after_modify = 0, mutations_sync = 2;

-- TTL not recalculated yet: the row is still present.
SELECT count() FROM apply_patches_materialized_chain;

-- Materializing the patch must recompute m2 -> m3 from the patched src and recalculate
-- the DELETE WHERE TTL, dropping the now-expired matching row (no merge needed).
ALTER TABLE apply_patches_materialized_chain APPLY PATCHES SETTINGS mutations_sync = 2;
SELECT count() FROM apply_patches_materialized_chain;

DROP TABLE apply_patches_materialized_chain;

-- CLEAR COLUMN must also feed dependency analysis: clearing a base column that a DELETE
-- WHERE MATERIALIZED column is derived from has to recalculate the TTL, otherwise the part
-- keeps its old rows_where_ttl_info and a row that only starts matching after the clear is
-- silently retained. Observe the count right after CLEAR COLUMN (an OPTIMIZE FINAL would
-- re-evaluate TTL from scratch and hide the bug).
DROP TABLE IF EXISTS clear_column_ttl_recalc;

CREATE TABLE clear_column_ttl_recalc
(
    d DateTime,
    src UInt8,
    flag UInt8 MATERIALIZED (src = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

-- src = 5 -> flag = 0, so the (expired) row does not match DELETE WHERE flag = 1 yet.
INSERT INTO clear_column_ttl_recalc (d, src) VALUES ('2000-01-01 00:00:00', 5);
ALTER TABLE clear_column_ttl_recalc
    MODIFY TTL d + INTERVAL 1 SECOND DELETE WHERE flag = 1
    SETTINGS materialize_ttl_after_modify = 0, mutations_sync = 2;

-- Row still present (TTL not recalculated yet).
SELECT count() FROM clear_column_ttl_recalc;

-- CLEAR src -> src = 0 -> flag recomputes to 1, so the expired row now matches DELETE WHERE
-- and the mutation must drop it (count 0), not keep stale TTL metadata (count 1).
ALTER TABLE clear_column_ttl_recalc CLEAR COLUMN src SETTINGS mutations_sync = 2;
SELECT count() FROM clear_column_ttl_recalc;

DROP TABLE clear_column_ttl_recalc;

-- A projection or skip index reading only a derived MATERIALIZED column (m2 in src -> m1 -> m2)
-- must be rebuilt when a mutation rewrites m2, otherwise MutateTask hardlinks the old
-- projection/index files and queries read stale data even though the base part is correct.
DROP TABLE IF EXISTS derived_materialized_projection_index;

CREATE TABLE derived_materialized_projection_index
(
    id UInt64,
    src UInt64,
    m1 UInt64 MATERIALIZED src,
    m2 UInt64 MATERIALIZED m1,
    INDEX ix m2 TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT m2, count() GROUP BY m2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;

INSERT INTO derived_materialized_projection_index (id, src) VALUES (1, 5), (2, 5), (3, 7);

-- Rewrite src for id = 1; m1 and m2 (which only the projection/index read) must recompute to 7.
ALTER TABLE derived_materialized_projection_index UPDATE src = 7 WHERE id = 1 SETTINGS mutations_sync = 2;

-- Projection must agree with the base part: m2 = 5 -> 1 row, m2 = 7 -> 2 rows.
SELECT m2, count() FROM derived_materialized_projection_index
GROUP BY m2 ORDER BY m2
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT m2, count() FROM derived_materialized_projection_index
GROUP BY m2 ORDER BY m2
SETTINGS optimize_use_projections = 0;

-- Skip index on the derived column must be rebuilt too: a stale minmax [5,5] granule for
-- id = 1 would wrongly prune the row whose m2 became 7. Expect 2 (id = 1 and id = 3).
SELECT count() FROM derived_materialized_projection_index WHERE m2 = 7;

DROP TABLE derived_materialized_projection_index;

-- The DELETE WHERE predicate may read the cleared base column directly, without a
-- MATERIALIZED hop. A non-default DEFAULT pins which value the TTL is evaluated against:
-- the post-clear default (7), not the stale stored value (5).
DROP TABLE IF EXISTS clear_column_direct_ttl_recalc;

CREATE TABLE clear_column_direct_ttl_recalc
(
    id UInt8,
    d DateTime,
    src UInt8 DEFAULT 7
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 SECOND DELETE WHERE src = 7
SETTINGS min_bytes_for_wide_part = 0;

-- id = 1 is expired and starts matching only after the clear; id = 2 is not expired;
-- id = 3 is expired but already matches, so it is TTL-eligible before the clear.
INSERT INTO clear_column_direct_ttl_recalc (id, d, src) VALUES
    (1, '2000-01-01 00:00:00', 5), (2, '2099-01-01 00:00:00', 5), (3, '2000-01-01 00:00:00', 7);

-- Clearing src resets it to 7, so id = 1 now matches DELETE WHERE and, being expired, must
-- be dropped by the mutation. id = 2 must survive: it matches the predicate but is not
-- expired, which also proves the TTL info was recalculated rather than blanket-applied.
ALTER TABLE clear_column_direct_ttl_recalc CLEAR COLUMN src SETTINGS mutations_sync = 2;
SELECT id, src FROM clear_column_direct_ttl_recalc ORDER BY id;

-- A row matching neither the stored nor the default value must be retained even when expired.
DROP TABLE IF EXISTS clear_column_direct_ttl_no_match;

CREATE TABLE clear_column_direct_ttl_no_match
(
    d DateTime,
    src UInt8 DEFAULT 7
)
ENGINE = MergeTree
ORDER BY tuple()
TTL d + INTERVAL 1 SECOND DELETE WHERE src = 99
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO clear_column_direct_ttl_no_match (d, src) VALUES ('2000-01-01 00:00:00', 5);
ALTER TABLE clear_column_direct_ttl_no_match CLEAR COLUMN src SETTINGS mutations_sync = 2;
SELECT count() FROM clear_column_direct_ttl_no_match;

DROP TABLE clear_column_direct_ttl_recalc;
DROP TABLE clear_column_direct_ttl_no_match;

-- CLEAR COLUMN recomputes only MATERIALIZED columns that depend on the cleared
-- column. Unrelated MATERIALIZED columns and their artifacts remain untouched;
-- the metadata-only MODIFY below therefore does not rewrite existing value 1.
-- min_bytes_for_full_part_storage = 0 pins Full storage: the runner randomizes that threshold,
-- and a Packed part takes the all-column mutation path, which rebuilds the artifacts anyway
-- and would make the assertions below pass without exercising the rebuild selection.
-- enable_block_number_column and enable_block_offset_column are pinned off for the same reason
-- in the other direction: the runner randomizes both, and with either one on the mutation does
-- not recompute the MATERIALIZED columns at all (master behaves the same way), so the rows below
-- would describe a case the run never reached.
DROP TABLE IF EXISTS clear_column_unrelated_materialized;

CREATE TABLE clear_column_unrelated_materialized
(
    id Int64,
    c Int64,
    d Int64 MATERIALIZED c + 1,
    other Int64 MATERIALIZED 1,
    PROJECTION p (SELECT other, count() GROUP BY other)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1, min_bytes_for_full_part_storage = 0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO clear_column_unrelated_materialized (id, c) VALUES (1, 10), (2, 20);

ALTER TABLE clear_column_unrelated_materialized MODIFY COLUMN other Int64 MATERIALIZED 2 SETTINGS mutations_sync = 2;
-- No mutation is scheduled by the metadata-only MODIFY, so the part still holds other = 1.
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 'clear_column_unrelated_materialized';

ALTER TABLE clear_column_unrelated_materialized CLEAR COLUMN c SETTINGS mutations_sync = 2;

-- The base part keeps unrelated `other = 1` and only recomputes c/d.
SELECT id, c, d, other FROM clear_column_unrelated_materialized ORDER BY id;
-- The projection remains consistent with the untouched stored value.
SELECT other, count() FROM clear_column_unrelated_materialized
GROUP BY other ORDER BY other SETTINGS force_optimize_projection = 1;

DROP TABLE clear_column_unrelated_materialized;

-- Same for a skip index on an unrelated MATERIALIZED column.
DROP TABLE IF EXISTS clear_column_unrelated_materialized_index;

CREATE TABLE clear_column_unrelated_materialized_index
(
    id Int64,
    c Int64,
    d Int64 MATERIALIZED c + 1,
    other Int64 MATERIALIZED 1,
    INDEX ix other TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1, min_bytes_for_full_part_storage = 0,
         enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO clear_column_unrelated_materialized_index (id, c) VALUES (1, 10), (2, 20);

ALTER TABLE clear_column_unrelated_materialized_index MODIFY COLUMN other Int64 MATERIALIZED 2 SETTINGS mutations_sync = 2;
ALTER TABLE clear_column_unrelated_materialized_index CLEAR COLUMN c SETTINGS mutations_sync = 2;

SELECT id, c, d, other FROM clear_column_unrelated_materialized_index ORDER BY id;
SELECT count() FROM clear_column_unrelated_materialized_index
WHERE other = 2 SETTINGS force_data_skipping_indices = 'ix';
SELECT count() FROM clear_column_unrelated_materialized_index
WHERE other = 1 SETTINGS force_data_skipping_indices = 'ix';

DROP TABLE clear_column_unrelated_materialized_index;
