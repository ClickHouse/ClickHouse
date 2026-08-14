-- Test that asterisk_include_virtual_columns includes virtual columns in SELECT * for Memory table.

set enable_analyzer = 1;

DROP TABLE IF EXISTS test_virtuals_memory;

CREATE TABLE test_virtuals_memory
(
    id UInt64,
    value String,
    alias ALIAS concat('Alias_', toString(id))
) ENGINE = Memory;

INSERT INTO test_virtuals_memory VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- { echoOn }

-- Without the setting, SELECT * returns only ordinary columns.
SELECT * FROM test_virtuals_memory ORDER BY id;

-- With the setting, SELECT * also returns virtual columns (_table for Memory).
SELECT * FROM test_virtuals_memory ORDER BY id SETTINGS asterisk_include_virtual_columns = 1;

-- Qualified asterisk should also work.
SELECT t.* FROM test_virtuals_memory AS t ORDER BY id SETTINGS asterisk_include_virtual_columns = 1;

-- COLUMNS matcher should not be affected.
SELECT COLUMNS('id|value') FROM test_virtuals_memory ORDER BY id SETTINGS asterisk_include_virtual_columns = 1;

-- Combined with other asterisk settings.
SELECT * FROM test_virtuals_memory ORDER BY id SETTINGS asterisk_include_virtual_columns = 1, asterisk_include_alias_columns = 1;

-- { echoOff }

DROP TABLE test_virtuals_memory;
