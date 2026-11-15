-- The sign column of CollapsingMergeTree can be modified while it keeps type Int8.

DROP TABLE IF EXISTS t_sign;

CREATE TABLE t_sign (key UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY key;
INSERT INTO t_sign VALUES (1, 1);

ALTER TABLE t_sign MODIFY COLUMN sign Int8 DEFAULT 1;
INSERT INTO t_sign (key) VALUES (2);
SELECT * FROM t_sign ORDER BY key;

ALTER TABLE t_sign MODIFY COLUMN sign UInt8; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_sign MODIFY COLUMN sign Int16; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_sign;
