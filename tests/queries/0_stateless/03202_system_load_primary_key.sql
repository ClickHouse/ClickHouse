-- Tags: no-parallel
DROP TABLE IF EXISTS test_load_primary_key;
DROP TABLE IF EXISTS test_load_primary_key_2;

-- Create the test tables
CREATE TABLE test_load_primary_key (id Int32, value String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE test_load_primary_key_2 (id Int32, value String) ENGINE = MergeTree() ORDER BY id;

-- Insert data into both tables
INSERT INTO test_load_primary_key VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO test_load_primary_key_2 VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- Check primary key memory before loading (this checks if it's not loaded yet) for both tables
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE database = currentDatabase()
AND table IN ('test_load_primary_key', 'test_load_primary_key_2');

-- Load primary keys for all tables in the database
SYSTEM LOAD PRIMARY KEY;

-- Verify primary key memory after loading for both tables
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE database = currentDatabase()
AND table IN ('test_load_primary_key', 'test_load_primary_key_2');

-- Unload primary keys for all tables in the database
SYSTEM UNLOAD PRIMARY KEY;

-- Verify primary key memory after unloading for both tables
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE database = currentDatabase()
AND table IN ('test_load_primary_key', 'test_load_primary_key_2');

-- Load primary key for only one table
SYSTEM LOAD PRIMARY KEY test_load_primary_key;

-- Verify that only one table's primary key is loaded
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE database = currentDatabase()
AND table IN ('test_load_primary_key', 'test_load_primary_key_2');

-- Select to verify the data is correctly loaded for both tables
SELECT * FROM test_load_primary_key ORDER BY id;
SELECT * FROM test_load_primary_key_2 ORDER BY id;
