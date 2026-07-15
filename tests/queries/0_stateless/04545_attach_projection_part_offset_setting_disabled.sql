-- Regression for issue #102445: a table with a `_part_offset` projection must remain
-- attachable after `allow_part_offset_column_in_projections` is disabled. The CREATE-time
-- feature gate in checkProperties must be skipped on ATTACH, otherwise the table becomes
-- permanently unattachable after DETACH / server restart.

DROP TABLE IF EXISTS t_04545;

CREATE TABLE t_04545 (a UInt64, b UInt64,
    PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS allow_part_offset_column_in_projections = 1;

INSERT INTO t_04545 VALUES (1, 1), (2, 2);

ALTER TABLE t_04545 MODIFY SETTING allow_part_offset_column_in_projections = 0;

DETACH TABLE t_04545;
ATTACH TABLE t_04545;

SELECT count() FROM t_04545;

DROP TABLE t_04545;
