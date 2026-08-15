-- A dialect insert targets a schemaful ClickHouse table, so a top-level `_id` is kept when the
-- table has a real column for it. Only the top-level `_id` is the object id; a field of that name
-- inside a subdocument is an ordinary field.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (`_id` String, id Int32, name String, `profile._id` String) ENGINE = MergeTree ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.insertOne({"_id" : {"$oid" : "64c9c5b1e1b8a2b3c4d5e6f7"}, "id" : 1, "name" : "alpha", "profile" : {"_id" : "nested"}});
SET dialect='clickhouse';
SELECT _id, id, name, `profile._id` FROM docs ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.insertMany([{"_id" : 2, "id" : 2, "name" : "beta", "profile" : {"_id" : ""}}, {"_id" : 3, "id" : 3, "name" : "gamma", "profile" : {"_id" : ""}}]);
SET dialect='clickhouse';
SELECT _id, id, name FROM docs ORDER BY id;

DROP TABLE docs;
