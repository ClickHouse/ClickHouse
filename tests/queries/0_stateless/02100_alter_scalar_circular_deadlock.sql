DROP TABLE IF EXISTS foo;

CREATE TABLE foo (ts DateTime, x UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts)
ORDER BY (ts);

INSERT INTO foo (ts, x) SELECT toDateTime('2020-01-01 00:05:00'), number from system.numbers_mt LIMIT 10;

SET mutations_sync = 1;

ALTER TABLE foo UPDATE x = 1 WHERE x = (SELECT x from foo WHERE x = 4);

SELECT sum(x) == 42 FROM foo;

ALTER TABLE foo UPDATE x = 1 WHERE x IN (SELECT x FROM foo WHERE x != 0);

SELECT sum(x) == 9 FROM foo;
