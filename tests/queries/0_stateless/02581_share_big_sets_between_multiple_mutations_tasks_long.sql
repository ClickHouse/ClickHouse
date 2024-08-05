-- Tags: long, no-debug, no-tsan, no-asan, no-ubsan, no-msan, no-parallel, no-sanitize-coverage

-- no-parallel because the sets use a lot of memory, which may interfere with other tests

DROP TABLE IF EXISTS 02581_trips;

CREATE TABLE 02581_trips(id UInt32, description String, id2 UInt32, PRIMARY KEY id) ENGINE=MergeTree ORDER BY id;

-- Make multiple parts
INSERT INTO 02581_trips SELECT number, '', number FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+10000000, '', number FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+20000000, '', number FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+30000000, '', number FROM numbers(10000);

SELECT count() from 02581_trips WHERE description = '';

SELECT name FROM system.parts WHERE database=currentDatabase() AND table = '02581_trips' AND active ORDER BY name;

-- Start multiple mutations simultaneously
SYSTEM STOP MERGES 02581_trips;
ALTER TABLE 02581_trips UPDATE description='5' WHERE id IN (SELECT (number*10 + 5)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=0;
ALTER TABLE 02581_trips UPDATE description='6' WHERE id IN (SELECT (number*10 + 6)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=0;
ALTER TABLE 02581_trips DELETE WHERE id IN (SELECT (number*10 + 7)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=0;
ALTER TABLE 02581_trips UPDATE description='8' WHERE id IN (SELECT (number*10 + 8)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=0;
SYSTEM START MERGES 02581_trips;
DELETE FROM 02581_trips WHERE id IN (SELECT (number*10 + 9)::UInt32 FROM numbers(200000000));
SELECT count(), _part from 02581_trips WHERE description = '' GROUP BY _part ORDER BY _part;

DROP TABLE 02581_trips;
