-- A dotted field can contain the names of cursor methods. Only a top-level `.limit(...)` or
-- `.skip(...)` after `find` is a cursor suffix; a field inside `.sort(...)` is query data.
SET dialect = 'clickhouse';
DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id UInt8, `profile.limit` UInt8, `profile.skip` UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs VALUES (1, 2, 1), (2, 1, 2);

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';
db.docs.find({}).sort({"profile.limit" : 1});
db.docs.find({}).sort({"profile.skip" : 1});

SET dialect = 'clickhouse';
DROP TABLE docs;
