-- A full scan of a complex-key dictionary returns every key with its own attributes, also when it is split
-- into many small blocks read by several streams, and when it stops early on LIMIT.

DROP DICTIONARY IF EXISTS dict_hashed;
DROP DICTIONARY IF EXISTS dict_sparse_hashed;
DROP DICTIONARY IF EXISTS dict_sharded_hashed;
DROP DICTIONARY IF EXISTS dict_hashed_array;
DROP DICTIONARY IF EXISTS dict_range_hashed;
DROP TABLE IF EXISTS src;

CREATE TABLE src (id UInt64, id_key String, value String, start Date, end Date) ENGINE = MergeTree ORDER BY id;
INSERT INTO src
    SELECT number, toString(number % 97), concat('value_', toString(number * 7)),
        toDate('2020-01-01') + number % 10, toDate('2020-02-01') + number % 10
    FROM numbers(1000);

CREATE DICTIONARY dict_hashed (id UInt64, id_key String, value String)
PRIMARY KEY id, id_key SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE DICTIONARY dict_sparse_hashed (id UInt64, id_key String, value String)
PRIMARY KEY id, id_key SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(0) LAYOUT(COMPLEX_KEY_SPARSE_HASHED());

CREATE DICTIONARY dict_sharded_hashed (id UInt64, id_key String, value String)
PRIMARY KEY id, id_key SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED(SHARDS 4));

CREATE DICTIONARY dict_hashed_array (id UInt64, id_key String, value String)
PRIMARY KEY id, id_key SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED_ARRAY());

CREATE DICTIONARY dict_range_hashed (id UInt64, id_key String, start Date, end Date, value String)
PRIMARY KEY id, id_key SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(0) LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN start MAX end);

SELECT 'source', count(), sum(cityHash64(id, id_key, value)) FROM src;
SELECT 'hashed', count(), sum(cityHash64(*)) FROM dict_hashed SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'sparse_hashed', count(), sum(cityHash64(*)) FROM dict_sparse_hashed SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'sharded_hashed', count(), sum(cityHash64(*)) FROM dict_sharded_hashed SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'hashed_array', count(), sum(cityHash64(*)) FROM dict_hashed_array SETTINGS max_block_size = 7, max_threads = 4;

SELECT 'source with ranges', count(), sum(cityHash64(start, end, id, id_key, value)) FROM src;
SELECT 'range_hashed', count(), sum(cityHash64(*)) FROM dict_range_hashed SETTINGS max_block_size = 7, max_threads = 4;

SELECT 'hashed limit', count() FROM (SELECT * FROM dict_hashed LIMIT 10) SETTINGS max_block_size = 7;
SELECT 'sparse_hashed limit', count() FROM (SELECT * FROM dict_sparse_hashed LIMIT 10) SETTINGS max_block_size = 7;
SELECT 'sharded_hashed limit', count() FROM (SELECT * FROM dict_sharded_hashed LIMIT 10) SETTINGS max_block_size = 7;
SELECT 'hashed_array limit', count() FROM (SELECT * FROM dict_hashed_array LIMIT 10) SETTINGS max_block_size = 7;
SELECT 'range_hashed limit', count() FROM (SELECT * FROM dict_range_hashed LIMIT 10) SETTINGS max_block_size = 7;

DROP DICTIONARY dict_hashed;
DROP DICTIONARY dict_sparse_hashed;
DROP DICTIONARY dict_sharded_hashed;
DROP DICTIONARY dict_hashed_array;
DROP DICTIONARY dict_range_hashed;
DROP TABLE src;
