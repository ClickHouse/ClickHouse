-- Tags: no-replicated-database, no-shared-merge-tree, no-parallel-replicas
-- SharedMergeTree doesn't support replace partition from MergeTree engine

-- REPLACE PARTITION must not resurrect the replaced-out parts after a restart.
-- Before the fix the old parts were only deactivated in memory (no covering part on disk),
-- so reloading parts from disk (here via DETACH/ATTACH, same code path as a server restart)
-- made them active again and the table returned both the replacement and the stale rows.

DROP TABLE IF EXISTS t_dst;
DROP TABLE IF EXISTS t_src;

CREATE TABLE t_dst (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE t_src (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k PARTITION BY k;

-- Keep the replaced-out part on disk until the simulated restart (otherwise background
-- cleanup may remove it first and hide the bug, which is exactly the reported workaround).
SYSTEM STOP CLEANUP t_dst;
SYSTEM STOP MERGES t_dst;

INSERT INTO t_dst VALUES (1, 1);
INSERT INTO t_src VALUES (1, 2);

ALTER TABLE t_dst REPLACE PARTITION ID '1' FROM t_src;

SELECT 'before restart';
SELECT * FROM t_dst ORDER BY v;

-- DETACH + ATTACH rebuilds the active part set from disk, just like a server restart.
DETACH TABLE t_dst;
ATTACH TABLE t_dst;

SELECT 'after restart';
SELECT * FROM t_dst ORDER BY v;

DROP TABLE t_dst;
DROP TABLE t_src;
