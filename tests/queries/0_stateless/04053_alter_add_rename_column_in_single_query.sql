-- Regression test: ALTER TABLE with ADD COLUMN followed by RENAME COLUMN in the
-- same statement caused "Cannot find column" exception because ADD COLUMN was not
-- applied to intermediate metadata when it didn't require a mutation stage.
-- https://github.com/ClickHouse/ClickHouse/issues/100328

CREATE TABLE t_100328 (v1 UInt32, v2 String, v3 Date) ENGINE = MergeTree() ORDER BY v1;
INSERT INTO t_100328 VALUES (1, 'hello', '2024-01-01');

ALTER TABLE t_100328 ADD COLUMN v4 String DEFAULT 'new', RENAME COLUMN v4 TO v5;
SELECT v1, v5 FROM t_100328;

ALTER TABLE t_100328 ADD COLUMN v6 Int32 DEFAULT 42, MODIFY COLUMN v2 LowCardinality(String), DROP COLUMN v3, RENAME COLUMN v6 TO v7;
SELECT v1, v2, v5, v7 FROM t_100328;

DROP TABLE t_100328;

-- Non-column ALTER chains: ensure intermediate metadata is updated for indexes and projections too.
CREATE TABLE t_100328_idx (k UInt32, v String) ENGINE = MergeTree() ORDER BY k;

ALTER TABLE t_100328_idx ADD INDEX idx1 v TYPE bloom_filter GRANULARITY 1, DROP INDEX idx1;
SELECT name FROM system.data_skipping_indices WHERE table = 't_100328_idx' AND database = currentDatabase();

ALTER TABLE t_100328_idx ADD INDEX idx2 v TYPE bloom_filter GRANULARITY 1;
ALTER TABLE t_100328_idx ADD INDEX idx3 k TYPE minmax GRANULARITY 1, DROP INDEX idx2;
SELECT name FROM system.data_skipping_indices WHERE table = 't_100328_idx' AND database = currentDatabase();

DROP TABLE t_100328_idx;

CREATE TABLE t_100328_proj (k UInt32, v UInt32) ENGINE = MergeTree() ORDER BY k;

ALTER TABLE t_100328_proj ADD PROJECTION proj1 (SELECT k, sum(v) GROUP BY k), DROP PROJECTION proj1;
SELECT name FROM system.projections WHERE table = 't_100328_proj' AND database = currentDatabase();

DROP TABLE t_100328_proj;
