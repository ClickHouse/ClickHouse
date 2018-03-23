DROP TABLE IF EXISTS test.count;

CREATE TABLE test.count (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test.count SELECT * FROM numbers(1234567);

SELECT count() FROM remote('127.0.0.{1,2}', test.count);
SELECT count() / 2 FROM remote('127.0.0.{1,2}', test.count);

DROP TABLE test.count;
