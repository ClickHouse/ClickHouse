SET alter_sync = 2;

DROP TABLE IF EXISTS t_03644_min_level_for_wide_part;
DROP TABLE IF EXISTS t_03644_min_level_for_wide_part_rmt;

-- Can produce initial parts with level 1
SET optimize_on_insert = 0;

CREATE TABLE t_03644_min_level_for_wide_part (x int) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 1;

INSERT INTO t_03644_min_level_for_wide_part VALUES (1);

SELECT level, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_03644_min_level_for_wide_part' AND active;

INSERT INTO t_03644_min_level_for_wide_part VALUES (2);

OPTIMIZE TABLE t_03644_min_level_for_wide_part FINAL;

SELECT level, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_03644_min_level_for_wide_part' AND active;

SET optimize_on_insert = 1;

CREATE TABLE t_03644_min_level_for_wide_part_rmt (x int, y int) ENGINE = ReplacingMergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 2;

INSERT INTO t_03644_min_level_for_wide_part_rmt SELECT number, number * 2 FROM numbers(10);

SELECT level, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_03644_min_level_for_wide_part_rmt' AND active;

INSERT INTO t_03644_min_level_for_wide_part_rmt VALUES (1, 2);

OPTIMIZE TABLE t_03644_min_level_for_wide_part_rmt FINAL;

SELECT level, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_03644_min_level_for_wide_part_rmt' AND active;

DROP TABLE t_03644_min_level_for_wide_part;
DROP TABLE t_03644_min_level_for_wide_part_rmt;
