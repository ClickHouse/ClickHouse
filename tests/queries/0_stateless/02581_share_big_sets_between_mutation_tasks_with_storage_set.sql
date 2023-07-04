DROP TABLE IF EXISTS 02581_trips;

CREATE TABLE 02581_trips(id UInt32, description String, id2 UInt32, PRIMARY KEY id) ENGINE=MergeTree ORDER BY id;

-- Make multiple parts
INSERT INTO 02581_trips SELECT number, '', number FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+10000000, '', number FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+20000000, '', number FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+30000000, '', number FROM numbers(10000);

SELECT count() from 02581_trips WHERE description = '';


SELECT name FROM system.parts WHERE database=currentDatabase() AND table = '02581_trips' AND active ORDER BY name;

CREATE TABLE 02581_set (id UInt32) ENGINE = Set;

INSERT INTO 02581_set SELECT number*10+7 FROM numbers(10000000);

-- Run mutation with PK `id` IN big set
ALTER TABLE 02581_trips UPDATE description='d' WHERE id IN 02581_set SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

INSERT INTO 02581_set SELECT number*10+8 FROM numbers(10000000);

-- Run mutation with PK `id` IN big set after it is updated
ALTER TABLE 02581_trips UPDATE description='d' WHERE id IN 02581_set SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';


DROP TABLE 02581_set;
DROP TABLE 02581_trips;
