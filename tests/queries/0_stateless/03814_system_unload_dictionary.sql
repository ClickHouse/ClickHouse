-- Tags: no-parallel

DROP DICTIONARY IF EXISTS test_unload_dict;
DROP TABLE IF EXISTS test_unload_source;

-- Create source table
CREATE TABLE test_unload_source (id UInt64, value String) ENGINE = Memory;
INSERT INTO test_unload_source VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- Create dictionary
CREATE DICTIONARY test_unload_dict
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_unload_source'))
LAYOUT(FLAT())
LIFETIME(0);

-- 1. Load dictionary by querying it
SELECT dictGet('test_unload_dict', 'value', toUInt64(1));

-- Check dictionary is loaded
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

-- 2. Unload dictionary
SYSTEM UNLOAD DICTIONARY test_unload_dict;

-- Check dictionary is unloaded (status should be NOT_LOADED)
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

-- 3. Query dictionary again (should trigger lazy reload)
SELECT dictGet('test_unload_dict', 'value', toUInt64(2));

-- Check dictionary is loaded again after lazy reload
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

-- 4. Unload, then explicitly reload
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

SYSTEM RELOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

-- 5. Verify data is correct after reload (source data should be intact)
SELECT dictGet('test_unload_dict', 'value', toUInt64(3));

-- 6. Update source data, unload, then verify lazy reload picks up new data
INSERT INTO test_unload_source VALUES (4, 'four');
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT dictGet('test_unload_dict', 'value', toUInt64(4));

-- 7. Test SYSTEM UNLOAD DICTIONARIES (unload all)
SYSTEM UNLOAD DICTIONARIES;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

-- Cleanup
DROP DICTIONARY test_unload_dict;
DROP TABLE test_unload_source;
