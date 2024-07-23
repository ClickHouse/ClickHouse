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
