DROP DICTIONARY IF EXISTS test_unload_dict;
DROP TABLE IF EXISTS test_unload_source;

CREATE TABLE test_unload_source (id UInt64, value String) ENGINE = Memory;
INSERT INTO test_unload_source VALUES (1, 'one'), (2, 'two'), (3, 'three');

CREATE DICTIONARY test_unload_dict
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_unload_source'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT '1. Load dictionary by querying it, then check it is LOADED';
SELECT dictGet('test_unload_dict', 'value', toUInt64(1));
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

SELECT '2. Unload dictionary, then check it is NOT_LOADED';
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

SELECT '3. Query again triggers lazy reload, then check it is LOADED';
SELECT dictGet('test_unload_dict', 'value', toUInt64(2));
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

SELECT '4. Unload, then explicitly reload';
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';
SYSTEM RELOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

SELECT '5. Data is still correct after reload';
SELECT dictGet('test_unload_dict', 'value', toUInt64(3));

SELECT '6. Lazy reload picks up updated source data';
INSERT INTO test_unload_source VALUES (4, 'four');
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT dictGet('test_unload_dict', 'value', toUInt64(4));

SELECT '7. Unloading an already unloaded dictionary keeps it NOT_LOADED';
SYSTEM RELOAD DICTIONARY test_unload_dict;
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

SELECT '8. Unloading a non-existent dictionary fails';
SYSTEM UNLOAD DICTIONARY fake_dictionary; -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY test_unload_dict;
DROP TABLE test_unload_source;
