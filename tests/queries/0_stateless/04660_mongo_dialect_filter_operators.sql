-- The query operators a filter of a `find`, a `delete`, an `update` or a `$match` supports. The
-- set is the one SingleStore Kai, the other MongoDB compatibility layer over a SQL engine, lists
-- as supported, so that an application written against that one is not surprised here.
--
-- The queries below are, in order: `$eq` and `$ne`; the ordering comparisons; `$in` and `$nin`;
-- `$and`, `$or`, `$nor` and `$not`; `$exists`, which asks whether the column holds a value rather
-- than whether it is declared, because the schema of a table is fixed; `$mod`; `$regex` with and
-- without options; `$expr`, which compares two fields of the same document; `$size`, `$all` and
-- `$elemMatch` on an array; the bitwise operators, whose mask is a number or the list of the bit
-- positions that make it up; `$comment`, which carries no condition; and finally the operators
-- that are not supported, which have to be an error rather than a silently dropped condition.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

-- A single thread makes the Memory-engine read order deterministic: a `find` has no `ORDER BY`.
SET max_threads = 1;

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, name String, score Nullable(Int32), flags UInt32, tags Array(String), sizes Array(Int32)) ENGINE = Memory;
INSERT INTO docs VALUES (1, 'alpha', 10, 5, ['red', 'green'], [1, 2, 3]), (2, 'beta', NULL, 12, ['green'], [7]), (3, 'gamma', 30, 0, [], [4, 5]);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.docs.find({"id" : 2});
db.docs.find({"id" : {"$eq" : 3}});
db.docs.find({"id" : {"$ne" : 1}});

db.docs.find({"id" : {"$gt" : 1, "$lte" : 2}});
db.docs.find({"id" : {"$lt" : 2}});
db.docs.find({"id" : {"$gte" : 3}});

db.docs.find({"name" : {"$in" : ["alpha", "gamma"]}});
db.docs.find({"name" : {"$nin" : ["alpha", "gamma"]}});

db.docs.find({"$and" : [{"id" : {"$gte" : 2}}, {"name" : {"$ne" : "gamma"}}]});
db.docs.find({"$or" : [{"id" : 1}, {"id" : 3}]});
db.docs.find({"$nor" : [{"id" : 1}, {"id" : 3}]});
db.docs.find({"id" : {"$not" : {"$gt" : 1}}});

db.docs.find({"score" : {"$exists" : true}});
db.docs.find({"score" : {"$exists" : false}});

db.docs.find({"id" : {"$mod" : [2, 1]}});

db.docs.find({"name" : {"$regex" : "^a"}});
db.docs.find({"name" : {"$regex" : "^A", "$options" : "i"}});

db.docs.find({"$expr" : {"$gt" : ["$flags", "$id"]}});

db.docs.find({"sizes" : {"$size" : 3}});
db.docs.find({"tags" : {"$all" : ["green"]}});
db.docs.find({"sizes" : {"$elemMatch" : {"$gt" : 4, "$lt" : 6}}});

db.docs.find({"flags" : {"$bitsAllSet" : 4}});
db.docs.find({"flags" : {"$bitsAllSet" : [2, 3]}});
db.docs.find({"flags" : {"$bitsAnySet" : [0]}});
db.docs.find({"flags" : {"$bitsAllClear" : [0, 1]}});
db.docs.find({"flags" : {"$bitsAnyClear" : [2]}});

db.docs.find({"id" : 1, "$comment" : "carries no condition"});

SET dialect='clickhouse';
DROP TABLE docs;
