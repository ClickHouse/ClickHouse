SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.pk;
CREATE TABLE test.pk (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 1);

INSERT INTO test.pk (x) VALUES (1), (2), (3);

SELECT x FROM test.pk ORDER BY x;
SELECT x FROM test.pk WHERE x >= 2 ORDER BY x;

ALTER TABLE test.pk MODIFY PRIMARY KEY (x);

SELECT x FROM test.pk ORDER BY x;
SELECT x FROM test.pk WHERE x >= 2 ORDER BY x;

ALTER TABLE test.pk ADD COLUMN y String, MODIFY PRIMARY KEY (x, y);

SELECT x, y FROM test.pk ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y = '' ORDER BY x, y;

INSERT INTO test.pk (x, y) VALUES (1, 'Hello'), (2, 'World'), (3, 'abc'), (4, 'def');

SELECT x, y FROM test.pk ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y > '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y >= '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x > 2 AND y > 'z' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE y < 'A' ORDER BY x, y;

DETACH TABLE test.pk;
ATTACH TABLE test.pk (d Date DEFAULT '2000-01-01', x UInt64, y String) ENGINE = MergeTree(d, (x, y), 1);

SELECT x, y FROM test.pk ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y > '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y >= '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x > 2 AND y > 'z' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE y < 'A' ORDER BY x, y;

SET max_rows_to_read = 3;
SELECT x, y FROM test.pk WHERE x > 2 AND y > 'z' ORDER BY x, y;
SET max_rows_to_read = 0;

OPTIMIZE TABLE test.pk;
SELECT x, y FROM test.pk;
SELECT x, y FROM test.pk ORDER BY x, y;

ALTER TABLE test.pk MODIFY PRIMARY KEY (x);

SELECT x, y FROM test.pk ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y > '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y >= '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x > 2 AND y > 'z' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE y < 'A' ORDER BY x, y;

DETACH TABLE test.pk;
ATTACH TABLE test.pk (d Date DEFAULT '2000-01-01', x UInt64, y String) ENGINE = MergeTree(d, (x), 1);

SELECT x, y FROM test.pk ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y > '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x >= 2 AND y >= '' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE x > 2 AND y > 'z' ORDER BY x, y;
SELECT x, y FROM test.pk WHERE y < 'A' ORDER BY x, y;

DROP TABLE test.pk;

DROP TABLE IF EXISTS test.pk2;
CREATE TABLE test.pk2 (x UInt32) ENGINE MergeTree ORDER BY x;

ALTER TABLE test.pk2 ADD COLUMN y UInt32, ADD COLUMN z UInt32, MODIFY ORDER BY (x, y, z);
ALTER TABLE test.pk2 MODIFY PRIMARY KEY (y); -- { serverError 36 }
ALTER TABLE test.pk2 MODIFY PRIMARY KEY (x, y);
SELECT '*** Check table creation statement ***';
SHOW CREATE TABLE test.pk2;

INSERT INTO test.pk2 VALUES (100, 30, 2), (100, 30, 1), (100, 20, 2), (100, 20, 1);
SELECT '*** Check that the inserted values were correctly sorted ***';
SELECT * FROM test.pk2;

DROP TABLE test.pk2;
