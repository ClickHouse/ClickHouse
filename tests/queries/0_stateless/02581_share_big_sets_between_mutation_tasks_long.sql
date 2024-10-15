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
ALTER TABLE 02581_trips UPDATE description='a' WHERE id IN (SELECT (number*10)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

ALTER TABLE 02581_trips UPDATE description='a' WHERE id IN (SELECT (number*10 + 1)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2, max_rows_in_set=1000;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with func(`id`) IN big subquery
ALTER TABLE 02581_trips UPDATE description='b' WHERE id::UInt64 IN (SELECT (number*10 + 2)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=2;
SELECT count() from 02581_trips WHERE description = '';

-- Run mutation with non-PK `id2` IN big subquery
--SELECT count(), _part FROM 02581_trips WHERE id2 IN (SELECT (number*10 + 3)::UInt32 FROM numbers(10000000)) GROUP BY _part ORDER BY _part;
--EXPLAIN SELECT (), _part FROM 02581_trips WHERE id2 IN (SELECT (number*10 + 3)::UInt32 FROM numbers(10000000));
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

SET max_rows_to_read = 0; -- system.text_log can be really big
SYSTEM FLUSH LOGS;
-- Check that in every mutation there were parts that built sets (log messages like 'Created Set with 10000000 entries from 10000000 rows in 0.388989187 sec.' )
-- and parts that shared sets (log messages like 'Got set from cache in 0.388930505 sec.' )
WITH (
        SELECT uuid
        FROM system.tables
        WHERE (database = currentDatabase()) AND (name = '02581_trips')
    ) AS table_uuid
SELECT
    CAST(splitByChar('_', query_id)[5], 'UInt64') AS mutation_version, -- '5521485f-8a40-4aba-87a2-00342c369563::all_3_3_0_6'
    sum(message LIKE 'Created Set with % entries%') >= 1  AS has_parts_for_which_set_was_built,
    sum(message LIKE 'Got set from cache%') >= 1 AS has_parts_that_shared_set
FROM system.text_log
WHERE
    query_id LIKE concat(CAST(table_uuid, 'String'), '::all\\_%')
    AND (event_date >= yesterday())
    AND (message LIKE 'Created Set with % entries%' OR message LIKE 'Got set from cache%')
GROUP BY mutation_version ORDER BY mutation_version FORMAT TSVWithNames;

DROP TABLE 02581_trips;
