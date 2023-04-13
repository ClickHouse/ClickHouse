DROP TABLE IF EXISTS 02581_trips;

CREATE TABLE 02581_trips(id UInt32, description String) ENGINE=MergeTree ORDER BY id;

-- Make multiple parts
INSERT INTO 02581_trips SELECT number, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+10000, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+20000, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+30000, '' FROM numbers(10000);

-- { echoOn }
SELECT count(), _part FROM 02581_trips GROUP BY _part ORDER BY _part;

-- Run mutation with a 'IN big subquery'
ALTER TABLE 02581_trips UPDATE description='1' WHERE id IN (SELECT (number*10+1)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2;
SELECT count(), _part FROM 02581_trips WHERE description = '' GROUP BY _part ORDER BY _part;
ALTER TABLE 02581_trips UPDATE description='2' WHERE id IN (SELECT (number*10+2)::UInt32 FROM numbers(10000)) SETTINGS mutations_sync=2;
SELECT count(), _part FROM 02581_trips WHERE description = '' GROUP BY _part ORDER BY _part;
-- { echoOff }

DROP TABLE 02581_trips;
