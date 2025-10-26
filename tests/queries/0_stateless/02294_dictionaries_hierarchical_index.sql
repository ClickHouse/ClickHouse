DROP TABLE IF EXISTS test_hierarchy_source_table;
CREATE TABLE test_hierarchy_source_table
(
    id UInt64,
    parent_id UInt64
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_hierarchy_source_table VALUES (1, 0);

DROP DICTIONARY IF EXISTS hierarchy_flat_dictionary_index;
CREATE DICTIONARY hierarchy_flat_dictionary_index
(
    id UInt64,
    parent_id UInt64 BIDIRECTIONAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarchy_source_table'))
LAYOUT(FLAT())
LIFETIME(0); -- {serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS hierarchy_flat_dictionary_index;
CREATE DICTIONARY hierarchy_flat_dictionary_index
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL BIDIRECTIONAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarchy_source_table'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT * FROM hierarchy_flat_dictionary_index;
SELECT hierarchical_index_bytes_allocated > 0 FROM system.dictionaries WHERE name = 'hierarchy_flat_dictionary_index' AND database = currentDatabase();

DROP DICTIONARY hierarchy_flat_dictionary_index;

DROP DICTIONARY IF EXISTS hierarchy_hashed_dictionary_index;
CREATE DICTIONARY hierarchy_hashed_dictionary_index
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL BIDIRECTIONAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarchy_source_table'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT * FROM hierarchy_hashed_dictionary_index;
SELECT hierarchical_index_bytes_allocated > 0 FROM system.dictionaries WHERE name = 'hierarchy_hashed_dictionary_index' AND database = currentDatabase();
DROP DICTIONARY hierarchy_hashed_dictionary_index;

DROP DICTIONARY IF EXISTS hierarchy_hashed_array_dictionary_index;
CREATE DICTIONARY hierarchy_hashed_array_dictionary_index
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarchy_source_table'))
LAYOUT(HASHED_ARRAY())
LIFETIME(0);

SELECT * FROM hierarchy_hashed_array_dictionary_index;
SELECT hierarchical_index_bytes_allocated > 0 FROM system.dictionaries WHERE name = 'hierarchy_hashed_array_dictionary_index' AND database = currentDatabase();

DROP DICTIONARY hierarchy_hashed_array_dictionary_index;
DROP TABLE test_hierarchy_source_table;
