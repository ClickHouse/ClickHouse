DROP TABLE IF EXISTS hierarchy_source_table;
CREATE TABLE hierarchy_source_table
(
    id UInt64,
    parent_id UInt64
) ENGINE = TinyLog;

INSERT INTO hierarchy_source_table VALUES (1, 0), (2, 1), (3, 1), (4, 2);

DROP DICTIONARY IF EXISTS hierarchy_hashed_array_dictionary;
CREATE DICTIONARY hierarchy_hashed_array_dictionary
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'hierarchy_source_table'))
LAYOUT(HASHED_ARRAY())
LIFETIME(MIN 1 MAX 1000);

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierarchy_hashed_array_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierarchy_hashed_array_dictionary', number, number) FROM system.numbers LIMIT 6;
SELECT 'Get children';
SELECT dictGetChildren('hierarchy_hashed_array_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get all descendants';
SELECT dictGetDescendants('hierarchy_hashed_array_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get descendants at first level';
SELECT dictGetDescendants('hierarchy_hashed_array_dictionary', number, 1) FROM system.numbers LIMIT 6;

DROP DICTIONARY hierarchy_hashed_array_dictionary;

DROP TABLE hierarchy_source_table;
