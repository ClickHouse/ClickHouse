-- Tags: no-parallel, no-random-settings
-- no-parallel: test loads/unloads _all_ PKs in the system, affecting itself when it runs in parallel
-- no-random-settings: random settings may set 'use_primary_key_cache', changing the expected behavior

DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;

CREATE TABLE tab1 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
CREATE TABLE tab2 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;

SELECT '-- Insert data into columns';
INSERT INTO tab1 SELECT randomString(1000) FROM numbers(100000);
INSERT INTO tab2 SELECT randomString(1000) FROM numbers(100000);
SELECT (SELECT count() FROM tab1), (SELECT count() FROM tab2);

SELECT '-- Check primary key memory after inserting into both tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

SELECT '-- Unload primary keys for all tables in the database';
SYSTEM UNLOAD PRIMARY KEY;
SELECT 'OK';

SELECT '-- Check the primary key memory after unloading all tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

SELECT '-- Load primary key for all tables';
SYSTEM LOAD PRIMARY KEY;
SELECT 'OK';

SELECT '-- Check the primary key memory after loading all tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

SELECT '-- Unload primary keys for all tables in the database';
SYSTEM UNLOAD PRIMARY KEY;
SELECT 'OK';

SELECT '-- Check the primary key memory after unloading all tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

SELECT '-- Load primary key for only one table';
SYSTEM LOAD PRIMARY KEY tab1;
SELECT 'OK';

SELECT '-- Check the primary key memory after loading only one table';
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

DROP TABLE tab1;
DROP TABLE tab2;
