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
ALTER TABLE 02581_trips UPDATE description='5' WHERE id IN (SELECT (number*10 + 5)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=0;
ALTER TABLE 02581_trips UPDATE description='6' WHERE id IN (SELECT (number*10 + 6)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=0;
ALTER TABLE 02581_trips DELETE                 WHERE id IN (SELECT (number*10 + 7)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=0;
ALTER TABLE 02581_trips UPDATE description='8' WHERE id IN (SELECT (number*10 + 8)::UInt32 FROM numbers(10000000)) SETTINGS mutations_sync=0;
SYSTEM START MERGES 02581_trips;

-- Wait for mutations to finish
SELECT count() FROM 02581_trips SETTINGS select_sequential_consistency = 1;

DELETE FROM 02581_trips                        WHERE id IN (SELECT (number*10 + 9)::UInt32 FROM numbers(10000000)) SETTINGS lightweight_deletes_sync = 2;
SELECT count(), _part from 02581_trips WHERE description = '' GROUP BY _part ORDER BY _part SETTINGS select_sequential_consistency=1;

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
