DROP TABLE IF EXISTS t_merge_smallparts;

CREATE TABLE t_merge_smallparts (x UInt64) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_merge_smallparts VALUES (0);
INSERT INTO t_merge_smallparts VALUES (1);
INSERT INTO t_merge_smallparts VALUES (2);
INSERT INTO t_merge_smallparts VALUES (3);
INSERT INTO t_merge_smallparts VALUES (4);
INSERT INTO t_merge_smallparts VALUES (5);
INSERT INTO t_merge_smallparts VALUES (6);
INSERT INTO t_merge_smallparts VALUES (7);

-- 8 single-row parts before any merge.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_smallparts' AND active;

-- LIMIT 3 must merge exactly 3 of the 8 parts into 1, leaving 8 - 3 + 1 = 6 active parts.
-- (Prior to the part-count fix, this collapsed to merging only 2 parts regardless of LIMIT.)
OPTIMIZE TABLE t_merge_smallparts PARTITION ID 'all' MERGE SMALLPARTS LIMIT 3 SETTINGS optimize_throw_if_noop = 1;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_smallparts' AND active;

-- No LIMIT: merge as many of the remaining small parts as fit into one.
OPTIMIZE TABLE t_merge_smallparts PARTITION ID 'all' MERGE SMALLPARTS SETTINGS optimize_throw_if_noop = 1;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_smallparts' AND active;

-- No data was lost or duplicated across the two merges.
SELECT sum(x), count() FROM t_merge_smallparts;

-- Requires PARTITION.
OPTIMIZE TABLE t_merge_smallparts MERGE SMALLPARTS; -- { serverError BAD_ARGUMENTS }

-- Incompatible with FINAL/DEDUPLICATE/CLEANUP.
OPTIMIZE TABLE t_merge_smallparts PARTITION ID 'all' FINAL MERGE SMALLPARTS; -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_merge_smallparts PARTITION ID 'all' DEDUPLICATE MERGE SMALLPARTS; -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_merge_smallparts PARTITION ID 'all' CLEANUP MERGE SMALLPARTS; -- { serverError BAD_ARGUMENTS }

-- A single remaining part cannot be merged with MERGE SMALLPARTS.
OPTIMIZE TABLE t_merge_smallparts PARTITION ID 'all' MERGE SMALLPARTS SETTINGS optimize_throw_if_noop = 1; -- { serverError CANNOT_ASSIGN_OPTIMIZE }

DROP TABLE t_merge_smallparts;
