DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.buffer;
SET send_logs_level = 'error';

CREATE TABLE test.dst (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE test.buffer (x UInt64, y UInt64) ENGINE = Buffer(test, dst, 1, 99999, 99999, 1, 1, 99999, 99999);

INSERT INTO test.buffer VALUES (1, 100);
INSERT INTO test.buffer VALUES (2, 200);
INSERT INTO test.buffer VALUES (3, 300);
SELECT 'init';
SELECT * FROM test.dst ORDER BY x;
SELECT '-';
SELECT * FROM test.buffer ORDER BY x;

ALTER TABLE test.dst DROP COLUMN x, MODIFY COLUMN y String, ADD COLUMN z String DEFAULT 'DEFZ';

INSERT INTO test.buffer VALUES (4, 400);
SELECT 'alt';
SELECT * FROM test.dst ORDER BY y;
SELECT '-';
SELECT * FROM test.buffer ORDER BY x;

OPTIMIZE TABLE test.buffer;
SELECT 'opt';
SELECT * FROM test.dst ORDER BY y;
SELECT '-';
SELECT * FROM test.buffer ORDER BY x;

SET send_logs_level = 'warning';
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.buffer;
