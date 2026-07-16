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
