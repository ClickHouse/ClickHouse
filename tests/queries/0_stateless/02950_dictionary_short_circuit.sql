-- Tags: no-parallel

DROP TABLE IF EXISTS dictionary_source_table;
CREATE TABLE dictionary_source_table
(
    id UInt64,
    v1 String,
    v2 Nullable(String)
) ENGINE=TinyLog;

INSERT INTO dictionary_source_table VALUES (0, 'zero', 'zero'), (1, 'one', NULL);

DROP DICTIONARY IF EXISTS flat_dictionary;
CREATE DICTIONARY flat_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT 'Flat dictionary';
SELECT dictGetOrDefault('flat_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
SELECT dictGetOrDefault('flat_dictionary', 'v2', id+1, intDiv(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY flat_dictionary;


DROP DICTIONARY IF EXISTS hashed_dictionary;
CREATE DICTIONARY hashed_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

SELECT 'Hashed dictionary';
SELECT dictGetOrDefault('hashed_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
SELECT dictGetOrDefault('hashed_dictionary', 'v2', id+1, intDiv(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY hashed_dictionary;


DROP DICTIONARY IF EXISTS hashed_array_dictionary;
CREATE DICTIONARY hashed_array_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED_ARRAY());

SELECT 'Hashed array dictionary';
SELECT dictGetOrDefault('hashed_array_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
SELECT dictGetOrDefault('hashed_array_dictionary', 'v2', id+1, intDiv(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY hashed_array_dictionary;


DROP TABLE IF EXISTS range_dictionary_source_table;
CREATE TABLE range_dictionary_source_table
(
    id UInt64,
    start Date,
    end Nullable(Date),
    val Nullable(UInt64)
) ENGINE=TinyLog;

INSERT INTO range_dictionary_source_table VALUES (0, '2023-01-01', Null, Null), (1, '2022-11-09', '2022-12-08', 1);

DROP DICTIONARY IF EXISTS range_hashed_dictionary;
CREATE DICTIONARY range_hashed_dictionary
(
    id UInt64,
    start Date,
    end Nullable(Date),
    val Nullable(UInt64)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'range_dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end);

SELECT 'Range hashed dictionary';
SELECT dictGetOrDefault('range_hashed_dictionary', 'val', id, toDate('2023-01-02'), intDiv(NULL, id))
FROM range_dictionary_source_table;
DROP DICTIONARY range_hashed_dictionary;
DROP TABLE range_dictionary_source_table;


DROP DICTIONARY IF EXISTS cache_dictionary;
CREATE DICTIONARY cache_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

SELECT 'Cache dictionary';
SELECT dictGetOrDefault('cache_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
SELECT dictGetOrDefault('cache_dictionary', 'v2', id+1, intDiv(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY cache_dictionary;


DROP DICTIONARY IF EXISTS ssd_cache_dictionary;
CREATE DICTIONARY ssd_cache_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(SSD_CACHE(PATH '/fasttest-workspace/db-fasttest/user_files/test_dict'));
-- LAYOUT(SSD_CACHE(PATH '/home/ubuntu/custom/user_files/test_dict'));

SELECT 'SSD Cache dictionary';
SELECT dictGetOrDefault('ssd_cache_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
SELECT dictGetOrDefault('ssd_cache_dictionary', 'v2', id+1, intDiv(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY ssd_cache_dictionary;


DROP DICTIONARY IF EXISTS direct_dictionary;
CREATE DICTIONARY direct_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LAYOUT(DIRECT());

SELECT 'Direct dictionary';
SELECT dictGetOrDefault('direct_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
SELECT dictGetOrDefault('direct_dictionary', 'v2', id+1, intDiv(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY direct_dictionary;

DROP TABLE dictionary_source_table;