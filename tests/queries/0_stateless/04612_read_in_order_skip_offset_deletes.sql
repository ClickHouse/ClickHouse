-- Tags: no-random-merge-tree-settings

-- Reader-side delete filtering removes rows after the physical granule row counts are fixed, so the
-- OFFSET-skip read-in-order optimization must not use those counts to drop leading granules.

DROP TABLE IF EXISTS t_skip_offset_lwd;
CREATE TABLE t_skip_offset_lwd (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_skip_offset_lwd SELECT number FROM numbers(16);
DELETE FROM t_skip_offset_lwd WHERE k = 0;

SELECT 'lightweight delete, offset at first granule boundary';
SELECT k FROM t_skip_offset_lwd ORDER BY k LIMIT 3 OFFSET 4;
SELECT 'lightweight delete, offset at second granule boundary';
SELECT k FROM t_skip_offset_lwd ORDER BY k LIMIT 3 OFFSET 8;

DROP TABLE t_skip_offset_lwd;

DROP TABLE IF EXISTS t_skip_offset_mut;
CREATE TABLE t_skip_offset_mut (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_skip_offset_mut SELECT number FROM numbers(16);
SYSTEM STOP MERGES t_skip_offset_mut;
ALTER TABLE t_skip_offset_mut DELETE WHERE k = 1 SETTINGS mutations_sync = 0;

SELECT 'on-the-fly delete mutation, offset at first granule boundary';
SELECT k FROM t_skip_offset_mut ORDER BY k LIMIT 3 OFFSET 4 SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_skip_offset_mut;
