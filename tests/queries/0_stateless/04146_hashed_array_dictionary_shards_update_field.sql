-- Test: exercises `registerDictionaryArrayHashed` BAD_ARGUMENTS check
--   when LAYOUT(HASHED_ARRAY(SHARDS > 1)) is combined with UPDATE_FIELD.
-- Covers: src/Dictionaries/HashedArrayDictionary.cpp:1264 — branch
--   `if (source_ptr->hasUpdateField() && shards > 1)` is new in this change
--   and was previously untested for HASHED_ARRAY / COMPLEX_KEY_HASHED_ARRAY.

DROP TABLE IF EXISTS test_table_array_inc;
CREATE TABLE test_table_array_inc
(
    key UInt64,
    value UInt16,
    last_access DateTime
) ENGINE=Memory();

DROP TABLE IF EXISTS test_table_complex_array_inc;
CREATE TABLE test_table_complex_array_inc
(
    key_1 UInt64,
    key_2 UInt64,
    value UInt16,
    last_access DateTime
) ENGINE=Memory();

-- Simple key HASHED_ARRAY rejects SHARDS > 1 with UPDATE_FIELD source
DROP DICTIONARY IF EXISTS test_dict_array_shards_inc;
CREATE DICTIONARY test_dict_array_shards_inc
(
    key UInt64,
    value UInt16
) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE test_table_array_inc UPDATE_FIELD last_access))
LAYOUT(HASHED_ARRAY(SHARDS 10))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY test_dict_array_shards_inc; -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY test_dict_array_shards_inc;

-- Complex key COMPLEX_KEY_HASHED_ARRAY rejects SHARDS > 1 with UPDATE_FIELD source
DROP DICTIONARY IF EXISTS test_dict_complex_array_shards_inc;
CREATE DICTIONARY test_dict_complex_array_shards_inc
(
    key_1 UInt64,
    key_2 UInt64,
    value UInt16
) PRIMARY KEY key_1, key_2
SOURCE(CLICKHOUSE(TABLE test_table_complex_array_inc UPDATE_FIELD last_access))
LAYOUT(COMPLEX_KEY_HASHED_ARRAY(SHARDS 10))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY test_dict_complex_array_shards_inc; -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY test_dict_complex_array_shards_inc;

-- Sanity: HASHED_ARRAY without SHARDS (or SHARDS=1) accepts UPDATE_FIELD source
DROP DICTIONARY IF EXISTS test_dict_array_no_shards_inc;
CREATE DICTIONARY test_dict_array_no_shards_inc
(
    key UInt64,
    value UInt16
) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE test_table_array_inc UPDATE_FIELD last_access))
LAYOUT(HASHED_ARRAY())
LIFETIME(0);

SYSTEM RELOAD DICTIONARY test_dict_array_no_shards_inc;
SELECT element_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_dict_array_no_shards_inc';

DROP DICTIONARY test_dict_array_no_shards_inc;

DROP TABLE test_table_array_inc;
DROP TABLE test_table_complex_array_inc;
