-- An Extended JSON wrapper nested inside an embedded document is converted into the value it wraps
-- before the document becomes a `JSON` value, the same way the wire insert path converts it. So a
-- `{"$date": ...}` inside an element of an array is stored as the date it names rather than as a
-- `JSON` object with a `$`-named field, and the two Mongo surfaces write one and the same document.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

-- A date of a `JSON` value carries no declared time zone, so the text it reads back as is the one of
-- the session. The instant is the same in every session, which is what the parity is about.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS wrapper_parity;
CREATE TABLE wrapper_parity (id Int64, events Array(JSON)) ENGINE = MergeTree ORDER BY id;

SET allow_experimental_mongo_dialect = 1;
SET mutations_sync = 1;
SET dialect='mongo';

db.wrapper_parity.insertOne({"id": 1, "events": [{"name": "start", "when": {"$date": {"$numberLong": "0"}}}]});
db.wrapper_parity.insertOne({"id": 2, "events": [{"name": "legacy", "when": {"$date": 1546300800000}}]});
db.wrapper_parity.insertOne({"id": 3, "events": [{"name": "id", "ref": {"$oid": "5f2a1b3c4d5e6f7a8b9c0d1e"}, "price": {"$numberDecimal": "1.50"}}]});
db.wrapper_parity.updateMany({"id": 1}, {"$push": {"events": {"name": "stop", "when": {"$date": {"$numberLong": "1000"}}}}});

SET dialect='clickhouse';

-- The paths of the stored documents say it directly: a wrapper of the inserted document is gone,
-- rather than kept as a `when.$date.$numberLong` path of the `JSON` value.
SELECT id, arrayMap(event -> JSONAllPaths(event), events) FROM wrapper_parity ORDER BY id;

SELECT id, events FROM wrapper_parity ORDER BY id FORMAT JSONEachRow;

DROP TABLE wrapper_parity;
