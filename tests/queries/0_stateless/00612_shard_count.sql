-- Tags: shard

DROP TABLE IF EXISTS count;

CREATE TABLE count (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO count SELECT * FROM numbers(1234567);

SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), count);
SELECT count() / 2 FROM remote('127.0.0.{1,2}', currentDatabase(), count);

DROP TABLE count;
