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
