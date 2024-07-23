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

SYSTEM FLUSH LOGS;

-- Check that in every mutation there were parts where selected rows count then the size of big sets which will mean that sets were shared
-- Also check that there was at least one part that read more rows then the size of set which will mean that the 
WITH 10000000 AS rows_in_set
SELECT
    mutation_version,
    countIf(read_rows >= rows_in_set) >= 1 as has_parts_for_which_set_was_built,
    countIf(read_rows <= rows_in_set) >= 1 as has_parts_that_shared_set
FROM
(
    SELECT
        CAST(splitByChar('_', part_name)[5], 'UInt64') AS mutation_version,
        read_rows
    FROM system.part_log
    WHERE database = currentDatabase() and (event_date >= yesterday()) AND (`table` = '02581_trips') AND (event_type = 'MutatePart')
)
GROUP BY mutation_version ORDER BY mutation_version FORMAT TSVWithNames;

DROP TABLE 02581_trips;
