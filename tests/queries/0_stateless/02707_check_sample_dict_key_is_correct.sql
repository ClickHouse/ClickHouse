DROP DICTIONARY IF EXISTS test_sample_key_dict1;
DROP DICTIONARY IF EXISTS test_sample_key_dict2;
DROP table IF EXISTS test_sample_key_local;

-- create local table
CREATE TABLE test_sample_key_local
(
    `id` Int128,
    `name` String
)
ENGINE = Memory;


-- create DICTIONARY with default settings check_sample_dict_key_is_correct = 1
CREATE DICTIONARY test_sample_key_dict1
(
    `id` Int128,
    `name` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_sample_key_local'))
LIFETIME(MIN 0 MAX 300)
LAYOUT(HASHED()); -- { serverError 489 }


-- create DICTIONARY with settings check_sample_dict_key_is_correct = 0
CREATE DICTIONARY test_sample_key_dict2
(
    `id` Int128,
    `name` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_sample_key_local'))
LIFETIME(MIN 0 MAX 300)
LAYOUT(HASHED())
SETTINGS(check_sample_dict_key_is_correct = 0);


DROP DICTIONARY IF EXISTS test_sample_key_dict1;
DROP DICTIONARY IF EXISTS test_sample_key_dict2;
DROP table IF EXISTS test_sample_key_local;