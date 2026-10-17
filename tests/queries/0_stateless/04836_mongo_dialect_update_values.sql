-- The right hand side of an update operator is data, not an aggregation expression: a string that
-- starts with a dollar sign is stored as it is written rather than read as a field path, an array
-- is stored as an array, and a document assigns one column per field it names - the dotted path,
-- which is how a nested field of a document becomes a column everywhere else in the dialect.
--
-- An update is an `ALTER TABLE ... UPDATE`, which is a mutation, so each one is awaited with
-- `mutations_sync` before the result is read back.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';
SET mutations_sync = 2;

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, name String, other String, tags Array(String), `profile.name` String, `profile.age` Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs VALUES (1, 'alpha', 'beta', ['red'], 'x', 10);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({"id" : 1}, {"$set" : {"name" : "$other"}});
SET dialect='clickhouse';
SELECT id, name, other FROM docs ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({"id" : 1}, {"$set" : {"tags" : ["white", "black"]}});
SET dialect='clickhouse';
SELECT id, tags FROM docs ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({"id" : 1}, {"$set" : {"profile" : {"name" : "y", "age" : 20}}});
SET dialect='clickhouse';
SELECT id, `profile.name`, `profile.age` FROM docs ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({"id" : 1}, {"$set" : {"profile.name" : "z"}});
SET dialect='clickhouse';
SELECT id, `profile.name`, `profile.age` FROM docs ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({"id" : 1}, {"$push" : {"tags" : "$other"}});
SET dialect='clickhouse';
SELECT id, tags FROM docs ORDER BY id;

DROP TABLE docs;
