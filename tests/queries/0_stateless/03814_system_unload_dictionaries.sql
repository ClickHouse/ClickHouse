-- Tags: no-parallel
-- SYSTEM UNLOAD DICTIONARIES is server-global, so it interferes with dictionaries of other parallel tests.

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

SELECT '1. Load, then unload all dictionaries server-wide; ours must be NOT_LOADED';
SELECT dictGet('test_unload_dict', 'value', toUInt64(1));
SYSTEM UNLOAD DICTIONARIES;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

SELECT '2. Reload, unload all again, then unload the single dict too; still NOT_LOADED';
SYSTEM RELOAD DICTIONARY test_unload_dict;
SYSTEM UNLOAD DICTIONARIES;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';
SYSTEM UNLOAD DICTIONARY test_unload_dict;
SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_unload_dict';

DROP DICTIONARY test_unload_dict;
DROP TABLE test_unload_source;
