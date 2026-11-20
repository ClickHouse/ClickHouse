-- An `ALTER` must not leave a constraint that references a column which no longer exists.

DROP TABLE IF EXISTS t_constraint_drop;
CREATE TABLE t_constraint_drop (x UInt8, y UInt8, CONSTRAINT c CHECK y > x) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_constraint_drop DROP COLUMN y; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE t_constraint_drop ADD COLUMN z UInt8;
INSERT INTO t_constraint_drop VALUES (1, 2, 3);
INSERT INTO t_constraint_drop VALUES (2, 1, 3); -- { serverError VIOLATED_CONSTRAINT }
SELECT * FROM t_constraint_drop;
DROP TABLE t_constraint_drop;
