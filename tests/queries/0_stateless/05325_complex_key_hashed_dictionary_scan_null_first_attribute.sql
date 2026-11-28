-- A full scan of a complex-key hashed dictionary returns the keys whose first attribute is NULL,
-- also when it is split into many small blocks read by several streams.

DROP DICTIONARY IF EXISTS dict_null_first;
DROP DICTIONARY IF EXISTS dict_null_first_sharded;
DROP TABLE IF EXISTS src_null_first;

CREATE TABLE src_null_first (id UInt64, id_key String, value Nullable(String)) ENGINE = Memory;
INSERT INTO src_null_first
    SELECT number, toString(number % 97), if(number % 3 = 0, NULL, concat('value_', toString(number)))
    FROM numbers(1000);

CREATE DICTIONARY dict_null_first (id UInt64, id_key String, value Nullable(String))
PRIMARY KEY id, id_key SOURCE(CLICKHOUSE(TABLE 'src_null_first')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE DICTIONARY dict_null_first_sharded (id UInt64, id_key String, value Nullable(String))
PRIMARY KEY id, id_key SOURCE(CLICKHOUSE(TABLE 'src_null_first')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED(SHARDS 4));

SELECT 'source', count(), countIf(value IS NULL), sum(cityHash64(id, id_key, ifNull(value, ''))) FROM src_null_first;
SELECT 'hashed', count(), countIf(value IS NULL), sum(cityHash64(id, id_key, ifNull(value, '')))
FROM dict_null_first SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'sharded_hashed', count(), countIf(value IS NULL), sum(cityHash64(id, id_key, ifNull(value, '')))
FROM dict_null_first_sharded SETTINGS max_block_size = 7, max_threads = 4;

DROP DICTIONARY dict_null_first;
DROP DICTIONARY dict_null_first_sharded;
DROP TABLE src_null_first;
