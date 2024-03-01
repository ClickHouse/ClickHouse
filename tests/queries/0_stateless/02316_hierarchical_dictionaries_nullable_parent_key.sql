DROP TABLE IF EXISTS test_hierarhical_table;
CREATE TABLE test_hierarhical_table
(
    id UInt64,
    parent_id Nullable(UInt64)
) ENGINE=TinyLog;

INSERT INTO test_hierarhical_table VALUES (0, NULL), (1, 0), (2, 1), (3, NULL), (4, 2);

DROP DICTIONARY IF EXISTS hierachical_flat_dictionary;
CREATE DICTIONARY hierachical_flat_dictionary
(
    id UInt64,
    parent_id Nullable(UInt64) HIERARCHICAL
) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarhical_table'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT 'Flat dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierachical_flat_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierachical_flat_dictionary', number, number) FROM system.numbers LIMIT 6;
SELECT 'Get children';
SELECT dictGetChildren('hierachical_flat_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get all descendants';
SELECT dictGetDescendants('hierachical_flat_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get descendants at first level';
SELECT dictGetDescendants('hierachical_flat_dictionary', number, 1) FROM system.numbers LIMIT 6;

DROP DICTIONARY hierachical_flat_dictionary;

DROP DICTIONARY IF EXISTS hierachical_hashed_dictionary;
CREATE DICTIONARY hierachical_hashed_dictionary
(
    id UInt64,
    parent_id Nullable(UInt64) HIERARCHICAL
) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarhical_table'))
LAYOUT(HASHED())
LIFETIME(0);

SELECT 'Hashed dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierachical_hashed_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierachical_hashed_dictionary', number, number) FROM system.numbers LIMIT 6;
SELECT 'Get children';
SELECT dictGetChildren('hierachical_hashed_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get all descendants';
SELECT dictGetDescendants('hierachical_hashed_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get descendants at first level';
SELECT dictGetDescendants('hierachical_hashed_dictionary', number, 1) FROM system.numbers LIMIT 6;

DROP DICTIONARY hierachical_hashed_dictionary;

DROP DICTIONARY IF EXISTS hierachical_hashed_array_dictionary;
CREATE DICTIONARY hierachical_hashed_array_dictionary
(
    id UInt64,
    parent_id Nullable(UInt64) HIERARCHICAL
) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarhical_table'))
LAYOUT(HASHED_ARRAY())
LIFETIME(0);

SELECT 'HashedArray dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierachical_hashed_array_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierachical_hashed_array_dictionary', number, number) FROM system.numbers LIMIT 6;
SELECT 'Get children';
SELECT dictGetChildren('hierachical_hashed_array_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get all descendants';
SELECT dictGetDescendants('hierachical_hashed_array_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get descendants at first level';
SELECT dictGetDescendants('hierachical_hashed_array_dictionary', number, 1) FROM system.numbers LIMIT 6;

DROP DICTIONARY hierachical_hashed_array_dictionary;

DROP DICTIONARY IF EXISTS hierachical_cache_dictionary;
CREATE DICTIONARY hierachical_cache_dictionary
(
    id UInt64,
    parent_id Nullable(UInt64) HIERARCHICAL
) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarhical_table'))
LAYOUT(CACHE(SIZE_IN_CELLS 10))
LIFETIME(0);

SELECT 'Cache dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierachical_cache_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierachical_cache_dictionary', number, number) FROM system.numbers LIMIT 6;


DROP DICTIONARY hierachical_cache_dictionary;

DROP DICTIONARY IF EXISTS hierachical_direct_dictionary;
CREATE DICTIONARY hierachical_direct_dictionary
(
    id UInt64,
    parent_id Nullable(UInt64) HIERARCHICAL
) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_hierarhical_table'))
LAYOUT(DIRECT());

SELECT 'Direct dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierachical_direct_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierachical_direct_dictionary', number, number) FROM system.numbers LIMIT 6;

DROP DICTIONARY hierachical_direct_dictionary;

DROP TABLE test_hierarhical_table;
