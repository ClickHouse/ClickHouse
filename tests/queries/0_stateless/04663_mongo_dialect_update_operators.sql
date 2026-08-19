-- The update operators of an `updateMany`, following what SingleStore Kai supports. A row always
-- has a value for every column, so `$unset` and the field `$rename` leaves behind write the
-- default of the column type - the value an insert that leaves the field out writes.
--
-- An update is an `ALTER TABLE ... UPDATE`, which is a mutation, so each one is awaited with
-- `mutations_sync` before the result is read back. The operators that are not supported are in
-- `04664_mongo_dialect_unsupported`, because a `-- { clientError ... }` hint cannot be written
-- next to a query of a dialect whose queries have no comment syntax.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';
SET mutations_sync = 2;

DROP TABLE IF EXISTS people;
CREATE TABLE people (id Int32, name String, other String, age Int32, score Float64, seen DateTime, tags Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO people VALUES (1, 'alpha', '', 30, 1.5, '2000-01-01 00:00:00', ['red', 'green']), (2, 'beta', '', 40, 2.5, '2000-01-01 00:00:00', ['blue']);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$set" : {"name" : "renamed"}, "$inc" : {"age" : 5}});
SET dialect='clickhouse';
SELECT id, name, age FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 2}, {"$mul" : {"score" : 4}});
SET dialect='clickhouse';
SELECT id, score FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({}, {"$min" : {"age" : 35}, "$max" : {"score" : 5}});
SET dialect='clickhouse';
SELECT id, age, score FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$unset" : {"name" : ""}});
SET dialect='clickhouse';
SELECT id, name FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 2}, {"$rename" : {"name" : "other"}});
SET dialect='clickhouse';
SELECT id, name, other FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$push" : {"tags" : "blue"}});
SET dialect='clickhouse';
SELECT id, tags FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$addToSet" : {"tags" : "blue"}});
SET dialect='clickhouse';
SELECT id, tags FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$push" : {"tags" : {"$each" : ["white", "black"]}}});
SET dialect='clickhouse';
SELECT id, tags FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$pop" : {"tags" : 1}});
SET dialect='clickhouse';
SELECT id, tags FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$pop" : {"tags" : -1}});
SET dialect='clickhouse';
SELECT id, tags FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$pull" : {"tags" : "green"}});
SET dialect='clickhouse';
SELECT id, tags FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$pullAll" : {"tags" : ["blue", "white"]}});
SET dialect='clickhouse';
SELECT id, tags FROM people ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.people.updateMany({"id" : 1}, {"$currentDate" : {"seen" : true}});
SET dialect='clickhouse';
SELECT id, seen > '2020-01-01 00:00:00' FROM people ORDER BY id;

DROP TABLE people;
