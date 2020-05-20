DROP TABLE IF EXISTS alter_00394;
CREATE TABLE alter_00394 (d Date, k UInt64, i32 Int32, n Nested(ui8 UInt8, s String)) ENGINE=MergeTree(d, k, 8192);

INSERT INTO alter_00394 VALUES ('2015-01-01', 3, 30, [1,2,3], ['1','12','123']);
INSERT INTO alter_00394 VALUES ('2015-01-01', 2, 20, [1,2], ['1','12']);
INSERT INTO alter_00394 VALUES ('2015-01-01', 1, 10, [1], ['1']);

ALTER TABLE alter_00394 ADD COLUMN `n.i8` Array(Int8) AFTER i32;

SELECT `n.i8`, `n.ui8`, `n.s` FROM alter_00394 ORDER BY k;
SELECT `n.i8` FROM alter_00394 ORDER BY k;

OPTIMIZE TABLE alter_00394;

SELECT `n.i8` FROM alter_00394 ORDER BY k;

DROP TABLE IF EXISTS alter_00394;
