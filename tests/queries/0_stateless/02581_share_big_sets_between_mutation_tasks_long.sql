-- Tags: long, no-debug, no-tsan, no-asan, no-ubsan, no-msan, no-parallel

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

-- Run mutation with `id` a 'IN big subquery'
ALTER TABLE 02581_trips UPDATE description='a' WHERE id IN (SELECT (number*10)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

ALTER TABLE 02581_trips UPDATE description='a' WHERE id IN (SELECT (number*10 + 1)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=2, max_rows_in_set=1000;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with func(`id`) IN big subquery
ALTER TABLE 02581_trips UPDATE description='b' WHERE id::UInt64 IN (SELECT (number*10 + 2)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with non-PK `id2` IN big subquery
ALTER TABLE 02581_trips UPDATE description='c' WHERE id2 IN (SELECT (number*10 + 3)::UInt32 FROM numbers(200000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with PK and non-PK IN big subquery
ALTER TABLE 02581_trips UPDATE description='c'
WHERE
    (id IN (SELECT (number*10 + 4)::UInt32 FROM numbers(200000000))) OR
    (id2 IN (SELECT (number*10 + 4)::UInt32 FROM numbers(200000000)))
SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with PK and non-PK IN big subquery
ALTER TABLE 02581_trips UPDATE description='c'
WHERE
    (id::UInt64 IN (SELECT (number*10 + 5)::UInt32 FROM numbers(200000000))) OR
    (id2::UInt64 IN (SELECT (number*10 + 5)::UInt32 FROM numbers(200000000)))
SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with PK and non-PK IN big subquery
ALTER TABLE 02581_trips UPDATE description='c'
WHERE
    (id::UInt32 IN (SELECT (number*10 + 6)::UInt32 FROM numbers(200000000))) OR
    ((id2+1)::String IN (SELECT (number*10 + 6)::UInt32 FROM numbers(200000000)))
SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

DROP TABLE 02581_trips;
