DROP TABLE IF EXISTS t_03644_min_level_for_wide_part;

-- Can produce initial parts with level 1
SET optimize_on_insert = 0;

CREATE TABLE t_03644_min_level_for_wide_part (x int) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 2;

INSERT INTO t_03644_min_level_for_wide_part VALUES (1);

SELECT level, part_type FROM system.parts WHERE active AND  table = 't_03644_min_level_for_wide_part';

INSERT INTO t_03644_min_level_for_wide_part VALUES (2);

OPTIMIZE TABLE t_03644_min_level_for_wide_part FINAL;

SELECT level, part_type FROM system.parts WHERE active AND table = 't_03644_min_level_for_wide_part';

INSERT INTO t_03644_min_level_for_wide_part VALUES (3);

OPTIMIZE TABLE t_03644_min_level_for_wide_part FINAL;

SELECT level, part_type FROM system.parts WHERE active AND table = 't_03644_min_level_for_wide_part';

DROP TABLE t_03644_min_level_for_wide_part;
