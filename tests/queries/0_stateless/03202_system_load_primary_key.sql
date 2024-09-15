-- Tags: no-parallel
DROP TABLE IF EXISTS test_load_primary_key;

-- Create the test table
CREATE TABLE test_load_primary_key (id Int32, value String) ENGINE = MergeTree() ORDER BY id;

-- Inserting some data
INSERT INTO test_load_primary_key VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Check primary key memory before loading (this checks if it's not loaded yet)
SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE database = currentDatabase()
AND table = 'test_load_primary_key';

-- Load primary key
SYSTEM LOAD PRIMARY KEY test_load_primary_key;

-- Check primary key memory after loading
SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE database = currentDatabase()
AND table = 'test_load_primary_key';

-- Unload primary key
SYSTEM UNLOAD PRIMARY KEY test_load_primary_key;

-- Check primary key memory after unloading
SELECT round(primary_key_bytes_in_memory, -7), round(primary_key_bytes_in_memory_allocated, -7)
FROM system.parts
WHERE database = currentDatabase()
AND table = 'test_load_primary_key';

-- Select to verify the data is correctly loaded
SELECT * FROM test_load_primary_key ORDER BY id;
