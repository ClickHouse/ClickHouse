-- Tags: no-fasttest

-- Regression test for the dictionary path of https://github.com/ClickHouse/ClickHouse/issues/105773.
-- An empty collection name (or one containing NUL bytes) used to abort the server inside the C driver
-- when the dictionary source was loaded; now it must fail with BAD_ARGUMENTS.

DROP DICTIONARY IF EXISTS dict_mongo_empty_collection;
CREATE DICTIONARY dict_mongo_empty_collection (id UInt64, name String)
PRIMARY KEY id
SOURCE(MONGODB(URI 'mongodb://example.test:27017/db' COLLECTION ''))
LIFETIME(0)
LAYOUT(FLAT());
SELECT * FROM dict_mongo_empty_collection; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY dict_mongo_empty_collection;

CREATE DICTIONARY dict_mongo_empty_collection (id UInt64, name String)
PRIMARY KEY id
SOURCE(MONGODB(URI 'mongodb://example.test:27017/db' COLLECTION '\0'))
LIFETIME(0)
LAYOUT(FLAT());
SELECT * FROM dict_mongo_empty_collection; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY dict_mongo_empty_collection;
