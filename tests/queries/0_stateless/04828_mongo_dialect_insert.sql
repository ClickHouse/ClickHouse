-- `insertOne` and `insertMany` of the `mongo` dialect write into an existing table: the fields
-- of the documents name its columns, a subdocument writes the dotted columns its leaves name,
-- and the documents of one `insertMany` may spell the same fields in any order. A bare constant
-- filter also matches the elements of an array field, the way MongoDB equality does.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS insert_target;
CREATE TABLE insert_target (name String, age Int64, tags Array(String), `profile.city` String) ENGINE = MergeTree ORDER BY name;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.insert_target.insertOne({"name": "a", "age": 20, "tags": ["red", "blue"], "profile": {"city": "Amsterdam"}});
db.insert_target.insertMany([{"name": "b", "age": 30, "tags": [], "profile": {"city": "Berlin"}}, {"age": 40, "profile": {"city": "Cairo"}, "tags": ["red"], "name": "c"}]);

db.insert_target.find({}).sort({"age": 1});
db.insert_target.find({"tags": "red"}).sort({"age": 1});
db.insert_target.find({"tags": {"$eq": "blue"}});
db.insert_target.find({"tags": {"$ne": "red"}});
db.insert_target.find({"name": "a"});

SET dialect='clickhouse';
DROP TABLE insert_target;
