-- In MongoDB `.limit(0)` means no limit at all, so it must return every matching document
-- rather than none, which a literal `LIMIT 0` would.

-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs SELECT number + 1 FROM numbers(5);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.docs.find({}).sort({"id" : 1}).limit(0);
db.docs.find({"id" : {"$gt" : 3}}).sort({"id" : 1}).limit(0);
db.docs.find({}).sort({"id" : 1}).limit(0).skip(3);
db.docs.find({}).sort({"id" : 1}).limit(2);

SET dialect='clickhouse';

DROP TABLE docs;
