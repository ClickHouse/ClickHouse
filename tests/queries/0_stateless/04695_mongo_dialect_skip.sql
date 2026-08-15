-- A `find` supports `.skip(...)`, which drivers use for pagination. It becomes an `OFFSET`,
-- so it composes with `.limit(...)` and `.sort(...)` the way it does in MongoDB, where the
-- documents are sorted first, then skipped, and only then limited.

-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs SELECT number + 1 FROM numbers(10);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.docs.find({}).sort({"id" : 1}).skip(3);
db.docs.find({}).sort({"id" : 1}).skip(8).limit(5);
db.docs.find({}).sort({"id" : -1}).limit(2).skip(4);
db.docs.find({"id" : {"$gt" : 5}}).sort({"id" : 1}).skip(2).limit(2);
db.docs.find({}).sort({"id" : 1}).skip(0).limit(3);

SET dialect='clickhouse';

DROP TABLE docs;
