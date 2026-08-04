-- Tags: no-random-settings, no-random-merge-tree-settings

-- B63: MATERIALIZE PROJECTION over a table with HETEROGENEOUS projection coverage. The first part
-- predates ADD PROJECTION (it must BUILD the projection); a later part already has it (the mutation
-- CARRIES IT FORWARD). On a content-addressed disk the carried-forward projection part was registered
-- in-memory without its rows_count / index granularity (the hardlinked files are not yet committed, so
-- it cannot reload them from disk), so a projection-served SELECT read back NOTHING from that part and
-- silently dropped its rows from the aggregate. The fix copies the source projection part's already-loaded
-- read-time state. This oracle compares the projection-served aggregate against the non-projection one in
-- the same run, so it is correct on both a plain and a content-addressed default disk.

DROP TABLE IF EXISTS t_proj_cf;

CREATE TABLE t_proj_cf (k1 UInt32, k2 UInt32, k3 UInt32, value UInt32)
ENGINE = MergeTree ORDER BY tuple();

-- First part: NO projection yet.
INSERT INTO t_proj_cf SELECT 1, number % 2, number % 4, number FROM numbers(50000);

SYSTEM STOP MERGES t_proj_cf;

ALTER TABLE t_proj_cf ADD PROJECTION aaaa (SELECT k1, k2, k3, sum(value) GROUP BY k1, k2, k3);

-- Second part: built WITH the projection (INSERT after ADD PROJECTION).
INSERT INTO t_proj_cf SELECT 1, number % 2, number % 4, number FROM numbers(100000) LIMIT 50000, 100000;

SYSTEM START MERGES t_proj_cf;

ALTER TABLE t_proj_cf MATERIALIZE PROJECTION aaaa SETTINGS mutations_sync = 2;

SELECT 'count', count() FROM t_proj_cf;

SELECT 'no_projection', k1, k2, k3, sum(value) v
FROM t_proj_cf GROUP BY k1, k2, k3 ORDER BY k1, k2, k3
SETTINGS optimize_use_projections = 0;

SELECT 'with_projection', k1, k2, k3, sum(value) v
FROM t_proj_cf GROUP BY k1, k2, k3 ORDER BY k1, k2, k3;

-- Every active part must carry a non-empty projection part after MATERIALIZE.
SELECT 'projection_parts', countDistinct(parent_name), min(rows)
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cf' AND active;

DROP TABLE t_proj_cf;
