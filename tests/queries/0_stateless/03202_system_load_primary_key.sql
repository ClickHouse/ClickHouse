-- Test to ensure SYSTEM LOAD PRIMARY KEY works as expected
CREATE TABLE test_load_primary_key (id Int32, value String) ENGINE = MergeTree() ORDER BY id;

-- Inserting some data
INSERT INTO test_load_primary_key VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Load primary key
SYSTEM LOAD PRIMARY KEY test_load_primary_key;

-- Select to verify the data is correctly loaded
SELECT * FROM test_load_primary_key ORDER BY id;
