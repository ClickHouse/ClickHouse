-- An embedded document that is a value rather than a set of paths - an element of an array, or the
-- document `$push` and `$addToSet` append - is written as a `JSON` value, which is the shape the
-- wire protocol infers an `Array(JSON)` column for. So the same collection can be written through
-- the dialect and through the Mongo endpoint.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS embedded_documents;
CREATE TABLE embedded_documents (id Int64, events Array(JSON), `profile.notes` Array(JSON)) ENGINE = MergeTree ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET mutations_sync = 1;
SET dialect='mongo';

db.embedded_documents.insertOne({"id": 1, "events": [{"name": "start", "n": 1}], "profile": {"notes": [{"text": "hi"}]}});
db.embedded_documents.insertMany([{"id": 2, "events": [{"name": "a"}, {"name": "b"}], "profile": {"notes": []}}]);

db.embedded_documents.updateMany({"id": 1}, {"$push": {"events": {"name": "stop"}}});
db.embedded_documents.updateMany({"id": 1}, {"$push": {"events": {"$each": [{"name": "x"}, {"name": "y"}]}}});
db.embedded_documents.updateMany({"id": 2}, {"$set": {"events": [{"name": "reset"}]}});
db.embedded_documents.updateMany({"id": 2}, {"$addToSet": {"events": {"name": "reset"}}});
db.embedded_documents.updateMany({"id": 2}, {"$addToSet": {"events": {"name": "added"}}});

SET dialect='clickhouse';

SELECT id, events, `profile.notes` FROM embedded_documents ORDER BY id FORMAT JSONEachRow;

DROP TABLE embedded_documents;
