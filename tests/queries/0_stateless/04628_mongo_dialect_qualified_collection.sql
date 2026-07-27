-- Tags: no-parallel
-- The test creates its own databases, so it must not share them with other tests.

SET dialect = 'clickhouse';

-- Force a single thread so that the read order is deterministic: the `find` queries below
-- translate to `SELECT`s without an `ORDER BY`.
SET max_threads = 1;

DROP DATABASE IF EXISTS db_04628_first;
DROP DATABASE IF EXISTS db_04628_second;
CREATE DATABASE db_04628_first;
CREATE DATABASE db_04628_second;

-- The same collection name in two databases must address two different tables.
CREATE TABLE db_04628_first.users (id Int32, name String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE db_04628_second.users (id Int32, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO db_04628_first.users VALUES (1, 'first-one'), (2, 'first-two');
INSERT INTO db_04628_second.users VALUES (1, 'second-one'), (2, 'second-two');

SET dialect = 'mongo';

db_04628_first.users.find({});
db_04628_second.users.find({});

db_04628_first.users.find({"id" : 1});
db_04628_second.users.find({"id" : 1});

db_04628_first.users.find({"$projection" : {"who" : "name"}});

db_04628_first.users.deleteMany({"id" : 1});

SET dialect = 'clickhouse';

-- Only the collection of the named database was affected by the delete.
SELECT 'first', id, name FROM db_04628_first.users ORDER BY id;
SELECT 'second', id, name FROM db_04628_second.users ORDER BY id;

DROP DATABASE db_04628_first;
DROP DATABASE db_04628_second;
