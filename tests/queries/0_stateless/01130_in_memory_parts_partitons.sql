DROP TABLE IF EXISTS t2;

CREATE TABLE t2(id UInt32, a UInt64, s String)
    ENGINE = MergeTree ORDER BY a PARTITION BY id
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_wide_part = 2000;

SYSTEM STOP MERGES t2;

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

DETACH TABLE t2;
ATTACH TABLE t2;

SELECT * FROM t2 ORDER BY a;
SELECT '==================';

DROP TABLE IF EXISTS t3;

CREATE TABLE t3(id UInt32, a UInt64, s String)
    ENGINE = MergeTree ORDER BY a PARTITION BY id
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_wide_part = 2000;

INSERT INTO t3 VALUES (3, 6, 'cc'), (3, 7, 'dd');
ALTER TABLE t2 REPLACE PARTITION 3 FROM t3;
SELECT * FROM t2 ORDER BY a;
SELECT table, name, part_type FROM system.parts WHERE table = 't2' AND active AND database = currentDatabase() ORDER BY name;
SELECT table, name, part_type FROM system.parts WHERE table = 't3' AND active AND database = currentDatabase() ORDER BY name;
SELECT '==================';

ALTER TABLE t3 FREEZE PARTITION 3;
SELECT name, part_type, is_frozen FROM system.parts WHERE table = 't3' AND active AND database = currentDatabase() ORDER BY name;

DROP TABLE t2;
DROP TABLE t3;
