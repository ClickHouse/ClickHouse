DROP TABLE IF EXISTS t2;

CREATE TABLE t2(id UInt32, a UInt64, s String)
    ENGINE = MergeTree ORDER BY a PARTITION BY id
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_wide_part = 2000;

INSERT INTO t2 VALUES (1, 2, 'foo'), (1, 3, 'bar');
INSERT INTO t2 VALUES (2, 4, 'aa'), (2, 5, 'bb');
INSERT INTO t2 VALUES (3, 6, 'qq'), (3, 7, 'ww');

SELECT * FROM t2 ORDER BY a;
SELECT '==================';

ALTER TABLE t2 DROP PARTITION 1;
SELECT * FROM t2 ORDER BY a;
SELECT '==================';

ALTER TABLE t2 DETACH PARTITION 2;
SELECT * FROM t2 ORDER BY a;
SELECT '==================';

ALTER TABLE t2 ATTACH PARTITION 2;
SELECT * FROM t2 ORDER BY a;
SELECT name, part_type FROM system.parts WHERE table = 't2' AND active AND database = currentDatabase() ORDER BY name;
SELECT '==================';
