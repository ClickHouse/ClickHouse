DROP TABLE IF EXISTS alter;
CREATE TABLE alter (d Date, k UInt64, i32 Int32, n Nested(ui8 UInt8, s String)) ENGINE=MergeTree(d, k, 8192);

INSERT INTO alter VALUES ('2015-01-01', 3, 30, [1,2,3], ['1','12','123']);
INSERT INTO alter VALUES ('2015-01-01', 2, 20, [1,2], ['1','12']);
INSERT INTO alter VALUES ('2015-01-01', 1, 10, [1], ['1']);

ALTER TABLE alter ADD COLUMN `n.i8` Array(Int8) AFTER i32;

SELECT `n.i8`, `n.ui8`, `n.s` FROM alter ORDER BY k;
SELECT `n.i8` FROM alter ORDER BY k;

OPTIMIZE TABLE alter;

SELECT `n.i8` FROM alter ORDER BY k;

DROP TABLE IF EXISTS alter;
