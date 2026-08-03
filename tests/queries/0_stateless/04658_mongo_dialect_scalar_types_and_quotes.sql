-- The Mongo dialect must cover every scalar type the wire protocol insert path can create
-- a column from (bool, long, double - not only int and String), preserve apostrophes inside
-- double quoted string literals, accept single quoted string literals, and reject unknown or
-- malformed operator objects with a parse error instead of dereferencing a null parser.
-- It must also match `$regex` as a regular expression rather than as a `LIKE` pattern, and let
-- several operators on the same field all hold at once.
--
-- An empty operator document holds no condition at all, and an unknown operator would silently
-- drop one, so both are errors.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

-- Force a single thread so the Memory-engine read order is deterministic:
-- the `find` queries below translate to `SELECT`s without an `ORDER BY`.
SET max_threads = 1;

DROP TABLE IF EXISTS users;
CREATE TABLE users (id Int32, active Bool, score Float64, big Int64, name String) ENGINE = Memory;
INSERT INTO users VALUES (1, true, 1.5, 5000000000, 'O''Reilly'), (2, false, 2.5, -5000000000, 'plain');

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.users.find({"active" : true});
db.users.find({"active" : false});
db.users.find({"score" : 2.5});
db.users.find({"big" : 5000000000});
db.users.find({"big" : -5000000000});
db.users.find({"name" : "O'Reilly"});
db.users.find({'name' : 'plain'});
db.users.find({"active" : {"$ne" : true}});
db.users.find({"score" : {"$gt" : 2.0}});
db.users.find({"big" : {"$lt" : 0}});
db.users.find({"id" : {"$in" : [1, 2]}});
db.users.find({"id" : {"$nin" : [1]}});
db.users.find({"name" : {"$regex" : "^p"}});
db.users.find({"name" : {"$regex" : "^P", "$options" : "i"}});
db.users.find({"name" : {"$not" : {"$regex" : "^p"}}});
db.users.find({"big" : {"$numberLong" : "5000000000"}});
db.users.find({"id" : {"$gt" : 0, "$lt" : 2}});
