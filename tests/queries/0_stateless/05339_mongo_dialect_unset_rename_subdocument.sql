-- A subdocument is stored as one column per leaf, named by its dotted path, so `$unset` and
-- `$rename` of the subdocument in an `updateMany` of the `mongo` dialect apply to every leaf,
-- the same as the `update` command of the wire protocol does.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';
SET mutations_sync = 2;

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, `profile.name` String, `profile.age` Int32, `user.name` String, `user.age` Int32, note String) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs VALUES (1, 'alpha', 30, '', 0, 'x'), (2, 'beta', 40, '', 0, 'y');

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({"id" : 1}, {"$rename" : {"profile" : "user"}});
SET dialect='clickhouse';
SELECT * FROM docs ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({"id" : 2}, {"$unset" : {"profile" : ""}, "$set" : {"note" : "z"}});
SET dialect='clickhouse';
SELECT * FROM docs ORDER BY id;

-- A field that is a column itself is not expanded.
SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.updateMany({}, {"$unset" : {"note" : ""}});
SET dialect='clickhouse';
SELECT * FROM docs ORDER BY id;

DROP TABLE docs;
