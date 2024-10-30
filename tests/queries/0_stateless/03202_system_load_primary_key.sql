-- Tags: no-parallel

-- Tests statement SYSTEM LOAD PRIMARY KEY

DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;

-- Create the test tables
CREATE TABLE tab1 (id Int32, val String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE tab2 (id Int32, val String) ENGINE = MergeTree() ORDER BY id;

-- Insert data into both tables
INSERT INTO tab1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO tab2 VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- Check primary key memory before loading (this checks if it's not loaded yet) for both tables
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

-- Load primary keys for all tables in the database
SYSTEM LOAD PRIMARY KEY;

-- Verify primary key memory after loading for both tables
-- Ensure .reference file has non-zero values here to reflect expected primary key loading
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

-- Unload primary keys for all tables in the database
SYSTEM UNLOAD PRIMARY KEY;

-- Verify primary key memory after unloading for both tables
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

-- Load primary key for only one table
SYSTEM LOAD PRIMARY KEY tab1;

-- Verify that only one table's primary key is loaded
SELECT
    table,
    round(primary_key_bytes_in_memory, -7),
    round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE
    database = currentDatabase()
    AND table IN ('tab1', 'tab2')
ORDER BY table;

-- Select to verify the data is correctly loaded for both tables
SELECT * FROM tab1 ORDER BY id;
SELECT * FROM tab2 ORDER BY id;

DROP TABLE tab1;
DROP TABLE tab2;
