-- Tags: long, no-tsan, no-asan, no-ubsan, no-msan

DROP TABLE IF EXISTS 02581_trips;

CREATE TABLE 02581_trips(id UInt32, description String) ENGINE=MergeTree ORDER BY id;

-- Make multiple parts
INSERT INTO 02581_trips SELECT number, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+100000, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+200000, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+300000, '' FROM numbers(10000);

SELECT count() from 02581_trips;

SELECT name FROM system.parts WHERE database=currentDatabase() AND table = '02581_trips' AND active ORDER BY name;

-- Run mutation with a 'IN big subquery'
ALTER TABLE 02581_trips UPDATE description='' WHERE id IN (SELECT (number+5)::UInt32 FROM numbers(100000000)) SETTINGS mutations_sync=2;

DROP TABLE 02581_trips;
