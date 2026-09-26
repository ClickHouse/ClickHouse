-- Tags: no-fasttest

-- An empty collection name or a name containing NUL bytes used to abort the server inside the C driver
-- (assertion failure in `_mongoc_cursor_collection`: `collection_len > 0`).
-- See https://github.com/ClickHouse/ClickHouse/issues/105773.

-- Table function: 6-argument form `mongodb('host:port', database, collection, user, password, structure)`.
SELECT * FROM mongodb('test', 'db', '', 'user', 'pass', 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('test', 'db', '\0', 'user', 'pass', 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('test', 'db', 'col\0lection', 'user', 'pass', 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('test', 'db', '\0col', 'user', 'pass', 'x Int32'); -- { serverError BAD_ARGUMENTS }

-- Table function: 3-argument form `mongodb(uri, collection, structure)`.
SELECT * FROM mongodb('mongodb://some-cluster:27017/db', '', 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/db', '\0', 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/db', 'col\0lection', 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/db', '\0col', 'x Int32'); -- { serverError BAD_ARGUMENTS }

-- The same problem reachable via the storage engine: `CREATE TABLE ... ENGINE = MongoDB(...)`.
DROP TABLE IF EXISTS t_mongo_empty_collection;
CREATE TABLE t_mongo_empty_collection (x Int32) ENGINE = MongoDB('some-cluster:27017', 'db', '', 'user', 'pass'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mongo_empty_collection (x Int32) ENGINE = MongoDB('some-cluster:27017', 'db', '\0', 'user', 'pass'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mongo_empty_collection (x Int32) ENGINE = MongoDB('mongodb://some-cluster:27017/db', ''); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mongo_empty_collection (x Int32) ENGINE = MongoDB('mongodb://some-cluster:27017/db', '\0'); -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS t_mongo_empty_collection;
