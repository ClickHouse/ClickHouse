DROP TABLE IF EXISTS test.mergetree;
CREATE TABLE test.mergetree (x UInt8, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test.mergetree VALUES (1, 'Hello, world!');
SELECT * FROM test.mergetree;

ALTER TABLE test.mergetree ADD COLUMN y UInt8 DEFAULT 0;
INSERT INTO test.mergetree VALUES (2, 'Goodbye.', 3);
SELECT * FROM test.mergetree ORDER BY x;

SELECT s FROM test.mergetree PREWHERE x AND y ORDER BY s;
SELECT s, y FROM test.mergetree PREWHERE x AND y ORDER BY s;

DROP TABLE test.mergetree;
