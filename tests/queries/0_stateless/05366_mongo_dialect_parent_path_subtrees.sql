-- A subdocument is stored as one column per leaf, named by its dotted path. A projection, an
-- exclusion, an `$unset` or a `$set` of the subdocument therefore applies to every leaf under it,
-- and to nothing else: `profiles` is not under `profile`.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';
SET mutations_sync = 2;

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, `profile.name` String, `profile.age` Int32, profiles Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs VALUES (1, 'alpha', 30, 5), (2, 'beta', 40, 6);

SELECT 'find, inclusion and exclusion of a parent path';
SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.find({}, {"profile" : 1}).sort({"profile.name" : 1});
db.docs.find({}, {"profile" : 0}).sort({"id" : 1});
db.docs.find({}, {"profile.age" : 1, "id" : 1}).sort({"id" : 1});
SET dialect='clickhouse';

SELECT 'aggregate, $project, $unset and $set of a parent path';
SET dialect='mongo';
db.docs.aggregate([{"$project" : {"profile" : 1}}, {"$sort" : {"profile.name" : 1}}]);
db.docs.aggregate([{"$unset" : "profile"}, {"$sort" : {"id" : 1}}]);
db.docs.aggregate([{"$set" : {"profile" : {"name" : "replaced"}}}, {"$sort" : {"id" : 1}}]);
db.docs.aggregate([{"$set" : {"profile.name" : "replaced"}}, {"$sort" : {"id" : 1}}]);
SET dialect='clickhouse';

SELECT 'updateMany, $set of a parent path resets the leaves it does not name';
SET dialect='mongo';
db.docs.updateMany({"id" : 1}, {"$set" : {"profile" : {"name" : "gamma"}}});
SET dialect='clickhouse';
SELECT * FROM docs ORDER BY id;

DROP TABLE docs;
