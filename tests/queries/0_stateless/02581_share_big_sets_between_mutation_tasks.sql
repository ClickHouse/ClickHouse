-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-fasttest
-- no-fasttest: Slow test
-- no sanitizers: too slow sometimes

DROP TABLE IF EXISTS 02581_trips;

CREATE TABLE 02581_trips(id UInt32, id2 UInt32, description String) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- Make multiple parts
INSERT INTO 02581_trips SELECT number, number, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+10000, number+10000, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+20000, number+20000, '' FROM numbers(10000);
INSERT INTO 02581_trips SELECT number+30000, number+30000, '' FROM numbers(10000);

-- { echoOn }
SELECT count(), _part FROM 02581_trips GROUP BY _part ORDER BY _part;

-- Run mutation with a 'IN big subquery'
ALTER TABLE 02581_trips UPDATE description='1' WHERE id IN (SELECT (number*10+1)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2;
SELECT count(), _part FROM 02581_trips WHERE description = '' GROUP BY _part ORDER BY _part;
ALTER TABLE 02581_trips UPDATE description='2' WHERE id IN (SELECT (number*10+2)::UInt32 FROM numbers(10000)) SETTINGS mutations_sync=2;
SELECT count(), _part FROM 02581_trips WHERE description = '' GROUP BY _part ORDER BY _part;

-- Run mutation with `id 'IN big subquery'
ALTER TABLE 02581_trips UPDATE description='a' WHERE id IN (SELECT (number*10)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

ALTER TABLE 02581_trips UPDATE description='a' WHERE id IN (SELECT (number*10 + 1)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2, max_rows_in_set=1000;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with func(`id`) IN big subquery
ALTER TABLE 02581_trips UPDATE description='b' WHERE id::UInt64 IN (SELECT (number*10 + 2)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with non-PK `id2` IN big subquery
ALTER TABLE 02581_trips UPDATE description='c' WHERE id2 IN (SELECT (number*10 + 3)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with PK and non-PK IN big subquery
ALTER TABLE 02581_trips UPDATE description='c'
WHERE
    (id IN (SELECT (number*10 + 4)::UInt32 FROM numbers(10000000))) OR
    (id2 IN (SELECT (number*10 + 4)::UInt32 FROM numbers(10000000)))
SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with PK and non-PK IN big subquery
ALTER TABLE 02581_trips UPDATE description='c'
WHERE
    (id::UInt64 IN (SELECT (number*10 + 5)::UInt32 FROM numbers(10000000))) OR
    (id2::UInt64 IN (SELECT (number*10 + 5)::UInt32 FROM numbers(10000000)))
SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with PK and non-PK IN big subquery
ALTER TABLE 02581_trips UPDATE description='c'
WHERE
    (id::UInt32 IN (SELECT (number*10 + 6)::UInt32 FROM numbers(10000000))) OR
    ((id2+1)::String IN (SELECT (number*10 + 6)::UInt32 FROM numbers(10000000)))
SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';
-- { echoOff }

DROP TABLE 02581_trips;
