DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    key UInt64,
    value UInt16
) ENGINE=Memory() AS SELECT number, number FROM numbers(1e5);

DROP TABLE IF EXISTS test_table_nullable;
CREATE TABLE test_table_nullable
(
    key UInt64,
    value Nullable(UInt16)
) ENGINE=Memory() AS SELECT number, number % 2 == 0 ? NULL : number FROM numbers(1e5);

DROP TABLE IF EXISTS test_table_string;
CREATE TABLE test_table_string
(
    key String,
    value UInt16
) ENGINE=Memory() AS SELECT 'foo' || number::String, number FROM numbers(1e5);

DROP TABLE IF EXISTS test_table_complex;
CREATE TABLE test_table_complex
(
    key_1 UInt64,
    key_2 UInt64,
    value UInt16
) ENGINE=Memory() AS SELECT number, number, number FROM numbers(1e5);

DROP DICTIONARY IF EXISTS test_dictionary_10_shards;
CREATE DICTIONARY test_dictionary_10_shards
(
    key UInt64,
    value UInt16
) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE test_table))
LAYOUT(SPARSE_HASHED(SHARDS 10))
LIFETIME(0);

SHOW CREATE test_dictionary_10_shards;
SYSTEM RELOAD DICTIONARY test_dictionary_10_shards;
SELECT element_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_dictionary_10_shards';
SELECT count() FROM test_table WHERE dictGet('test_dictionary_10_shards', 'value', key) != value;

DROP DICTIONARY test_dictionary_10_shards;

DROP DICTIONARY IF EXISTS test_dictionary_10_shards_nullable;
CREATE DICTIONARY test_dictionary_10_shards_nullable
(
    key UInt64,
    value Nullable(UInt16)
) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE test_table_nullable))
LAYOUT(SPARSE_HASHED(SHARDS 10))
LIFETIME(0);

SHOW CREATE test_dictionary_10_shards_nullable;
SYSTEM RELOAD DICTIONARY test_dictionary_10_shards_nullable;
SELECT element_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_dictionary_10_shards_nullable';
SELECT count() FROM test_table_nullable WHERE dictGet('test_dictionary_10_shards_nullable', 'value', key) != value;

DROP DICTIONARY test_dictionary_10_shards_nullable;

DROP DICTIONARY IF EXISTS test_complex_dictionary_10_shards;
CREATE DICTIONARY test_complex_dictionary_10_shards
(
    key_1 UInt64,
    key_2 UInt64,
    value UInt16
) PRIMARY KEY key_1, key_2
SOURCE(CLICKHOUSE(TABLE test_table_complex))
LAYOUT(COMPLEX_KEY_SPARSE_HASHED(SHARDS 10))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY test_complex_dictionary_10_shards;
SHOW CREATE test_complex_dictionary_10_shards;
SELECT element_count FROM system.dictionaries WHERE database = currentDatabase() and name = 'test_complex_dictionary_10_shards';
SELECT count() FROM test_table_complex WHERE dictGet('test_complex_dictionary_10_shards', 'value', (key_1, key_2)) != value;

DROP DICTIONARY test_complex_dictionary_10_shards;

DROP DICTIONARY IF EXISTS test_dictionary_10_shards_string;
CREATE DICTIONARY test_dictionary_10_shards_string
(
    key String,
    value UInt16
) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE test_table_string))
LAYOUT(SPARSE_HASHED(SHARDS 10))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY test_dictionary_10_shards_string;

DROP DICTIONARY test_dictionary_10_shards_string;

DROP DICTIONARY IF EXISTS test_dictionary_10_shards_incremental;
CREATE DICTIONARY test_dictionary_10_shards_incremental
(
    key UInt64,
    value UInt16
) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE test_table_last_access UPDATE_FIELD last_access))
LAYOUT(SPARSE_HASHED(SHARDS 10))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY test_dictionary_10_shards_incremental; -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY test_dictionary_10_shards_incremental;

DROP TABLE test_table;
DROP TABLE test_table_nullable;
DROP TABLE test_table_string;
DROP TABLE test_table_complex;
