SET dialect='clickhouse';

DROP TABLE IF EXISTS mongo_dialect_insert_object_id;
CREATE TABLE mongo_dialect_insert_object_id (`_id` String, name String) ENGINE = MergeTree ORDER BY _id;

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.mongo_dialect_insert_object_id.insertOne({"_id": "explicit-id", "name": "alpha"});

SET dialect='clickhouse';
SELECT _id, name FROM mongo_dialect_insert_object_id;
DROP TABLE mongo_dialect_insert_object_id;
