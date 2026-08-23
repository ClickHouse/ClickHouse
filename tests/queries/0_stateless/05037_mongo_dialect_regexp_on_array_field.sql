-- A regular expression of a filter matches element wise on an array field, the same way `$eq`
-- and `$in` do: `{"tags": {"$regex": "^be"}}` selects a document whose `tags` holds `beta`.
-- A scalar field keeps the plain match.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect = 'clickhouse';

DROP TABLE IF EXISTS regexp_array;
CREATE TABLE regexp_array (_id Int64, name String, tags Array(String)) ENGINE = Memory;
INSERT INTO regexp_array VALUES (1, 'bertha', ['beta', 'gamma']), (2, 'carl', ['alpha']), (3, 'diana', []);

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';

db.regexp_array.find({"tags" : {"$regex" : "^be"}});

db.regexp_array.find({"tags" : {"$regularExpression" : {"pattern" : "^al", "options" : ""}}});

db.regexp_array.find({"name" : {"$regex" : "^be"}});

db.regexp_array.find({"tags" : {"$regex" : "^no"}});

SET dialect = 'clickhouse';
DROP TABLE regexp_array;
