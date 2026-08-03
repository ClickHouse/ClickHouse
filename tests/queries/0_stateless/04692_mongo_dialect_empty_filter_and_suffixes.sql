-- An empty filter, and the `.limit` / `.sort` suffixes of a `find`.
--
-- `deleteMany({})` deletes every document, which left `ASTDeleteQuery` with no predicate at all -
-- and both its formatting and `InterpreterDeleteQuery` walk that predicate unconditionally, so the
-- server segfaulted on a query any client can send. `updateMany({}, ...)` already spelled the same
-- thing as `WHERE 1`.
--
-- The suffixes are searched for as plain text, which used to be done over the whole query: a
-- document holding `.limit(` or `.sort(` in a value of its own then turned the user's data into a
-- real `LIMIT` or `ORDER BY`, and a `find` without a limit took the limit of a later query of the
-- same multi query. Only the text that follows the argument list of this `find` is searched now.
--
-- The queries below are, in order: a `find` that matches every document; the same one asking for a
-- name that holds `.limit(`, and for one that holds `.sort(`, both of which are data and name no
-- suffix; a `find` with no suffix standing before one that has a `.limit`, which it must not take;
-- that `.limit`; a `.sort` and a `.limit` together; and an `updateMany` and a `deleteMany` of an
-- empty filter, which write and delete every document.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

-- A single thread makes the read order deterministic: a `find` has no `ORDER BY`. An update and a
-- delete are mutations, so both are awaited with `mutations_sync` before the result is read back.
SET max_threads = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs VALUES (1, 'alpha'), (2, '.limit(1)'), (3, '.sort({"id": -1})');

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.find({"name" : {"$ne" : "nothing"}});
db.docs.find({"name" : ".limit(1)"});
db.docs.find({"name" : ".sort({\"id\": -1})"});
db.docs.find({"name" : {"$ne" : "nothing"}});
db.docs.find({"name" : {"$ne" : "nothing"}}).limit(1);
db.docs.find({"name" : {"$ne" : "nothing"}}).sort({"id" : -1}).limit(2);
db.docs.updateMany({}, {"$set" : {"name" : "same"}});
db.docs.find({"name" : "same"});
db.docs.deleteMany({});
db.docs.aggregate([{"$count" : "left"}]);

SET dialect='clickhouse';

DROP TABLE docs;
