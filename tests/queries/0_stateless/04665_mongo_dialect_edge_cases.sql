-- The edge cases of the operators, taken from the integration suite of FerretDB, which compares
-- itself against a real MongoDB server for each of them. They are the ones where the obvious
-- translation gives an answer MongoDB does not.
--
-- The queries below are, in order: `$all` of an empty array, which matches nothing while the
-- ClickHouse `hasAll` of an empty array holds for every row; `$all` with a value repeated, which
-- is the same as naming it once; `$size` of zero, which matches an empty array, and of a value no
-- array has, which matches nothing; `$size` of a negative or a fractional number, which matches
-- nothing rather than failing; a bit position written as a whole double, which names the same bit
-- as the integer; a mask of zero, which every value has clear; `$limit`, `$skip` and the size of
-- `$sample` written as whole doubles, which is what a driver sends for a number that has no
-- fractional part; `$skip` of zero; and a `$skip` past the end of the stream.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

-- A single thread makes the Memory-engine read order deterministic: a `find` has no `ORDER BY`.
SET max_threads = 1;

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, tags Array(String), flags UInt32) ENGINE = Memory;
INSERT INTO docs VALUES (1, ['red', 'green'], 5), (2, [], 12);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.docs.aggregate([{"$match" : {"tags" : {"$all" : []}}}, {"$count" : "c"}]);
db.docs.find({"tags" : {"$all" : ["red", "red", "red"]}});

db.docs.find({"tags" : {"$size" : 0}});
db.docs.aggregate([{"$match" : {"tags" : {"$size" : 7}}}, {"$count" : "c"}]);
db.docs.aggregate([{"$match" : {"tags" : {"$size" : -1}}}, {"$count" : "c"}]);
db.docs.aggregate([{"$match" : {"tags" : {"$size" : 2.1}}}, {"$count" : "c"}]);

db.docs.find({"flags" : {"$bitsAllSet" : [2.0]}});
db.docs.find({"flags" : {"$bitsAllClear" : 0}});

db.docs.aggregate([{"$limit" : 2.0}, {"$count" : "c"}]);
db.docs.aggregate([{"$skip" : 1.0}, {"$count" : "c"}]);
db.docs.aggregate([{"$skip" : 0}, {"$count" : "c"}]);
db.docs.aggregate([{"$sample" : {"size" : 2.0}}, {"$count" : "c"}]);
db.docs.aggregate([{"$skip" : 1000}, {"$count" : "c"}]);

SET dialect='clickhouse';
DROP TABLE docs;
