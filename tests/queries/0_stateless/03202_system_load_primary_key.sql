-- Tags: no-parallel
-- no-parallel: test loads/unloads PKs of all tables, this affects expected results if the test runs in parallel

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test2;

CREATE TABLE test (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1, use_primary_key_cache = 0;
CREATE TABLE test2 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1, use_primary_key_cache = 0;

SELECT '-- Insert data into columns';
INSERT INTO test SELECT randomPrintableASCII(100) FROM numbers(10000);
INSERT INTO test2 SELECT randomPrintableASCII(100) FROM numbers(10000);
SELECT (SELECT count() FROM test), (SELECT count() FROM test2);

SELECT '-- Check primary key memory after inserting into both tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -6),
    round(primary_key_bytes_in_memory_allocated, -6)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('test', 'test2')
ORDER BY table;

SELECT '-- Unload primary keys for all tables in the database';
SYSTEM UNLOAD PRIMARY KEY;
SELECT 'OK';

SELECT '-- Check the primary key memory after unloading all tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -6),
    round(primary_key_bytes_in_memory_allocated, -6)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('test', 'test2')
ORDER BY table;

SELECT '-- Load primary key for all tables';
SYSTEM LOAD PRIMARY KEY;
SELECT 'OK';

SELECT '-- Check the primary key memory after loading all tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -6),
    round(primary_key_bytes_in_memory_allocated, -6)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('test', 'test2')
ORDER BY table;

SELECT '-- Unload primary keys for all tables in the database';
SYSTEM UNLOAD PRIMARY KEY;
SELECT 'OK';

SELECT '-- Check the primary key memory after unloading all tables';
SELECT
    table,
    round(primary_key_bytes_in_memory, -6),
    round(primary_key_bytes_in_memory_allocated, -6)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('test', 'test2')
ORDER BY table;

SELECT '-- Load primary key for only one table';
SYSTEM LOAD PRIMARY KEY test;
SELECT 'OK';

SELECT '-- Check the primary key memory after loading only one table';
SELECT
    table,
    round(primary_key_bytes_in_memory, -6),
    round(primary_key_bytes_in_memory_allocated, -6)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('test', 'test2')
ORDER BY table;

DROP TABLE test;
DROP TABLE test2;
