DROP TABLE IF EXISTS test_table_array_inc;
CREATE TABLE test_table_array_inc
(
    key UInt64,
    value UInt16,
    last_access DateTime
) ENGINE=Memory();

DROP DICTIONARY IF EXISTS test_dict_array_shards_inc;
CREATE DICTIONARY test_dict_array_shards_inc
(
    key UInt64,
    value UInt16
) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE test_table_array_inc UPDATE_FIELD last_access))
LAYOUT(HASHED_ARRAY(SHARDS 10))
LIFETIME(0);

-- The SHARDS/UPDATE_FIELD combination is rejected when the dictionary loads, not when it is created.
SYSTEM RELOAD DICTIONARY test_dict_array_shards_inc; -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY test_dict_array_shards_inc;

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
SELECT status, element_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'test_dict_array_no_shards_inc';

DROP DICTIONARY test_dict_array_no_shards_inc;
DROP TABLE test_table_array_inc;
