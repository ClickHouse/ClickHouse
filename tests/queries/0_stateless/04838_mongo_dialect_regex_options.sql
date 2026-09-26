-- `$regexMatch` and `$regexFind` take the options of the match as a sibling field of the pattern,
-- so a case insensitive match must not be lowered as a case sensitive one. The Extended JSON form
-- of a regular expression carries its options inside itself and is accepted as well.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs VALUES (1, 'Abc'), (2, 'abc'), (3, 'zzz');

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.aggregate([{"$project" : {"id" : 1, "matched" : {"$regexMatch" : {"input" : "$name", "regex" : "^a", "options" : "i"}}}}, {"$sort" : {"id" : 1}}]);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.aggregate([{"$project" : {"id" : 1, "matched" : {"$regexMatch" : {"input" : "$name", "regex" : "^a"}}}}, {"$sort" : {"id" : 1}}]);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.aggregate([{"$project" : {"id" : 1, "matched" : {"$regexMatch" : {"input" : "$name", "regex" : {"$regularExpression" : {"pattern" : "^A", "options" : "i"}}}}}}, {"$sort" : {"id" : 1}}]);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.aggregate([{"$project" : {"id" : 1, "found" : {"$regexFind" : {"input" : "$name", "regex" : "^(a)", "options" : "i"}}}}, {"$sort" : {"id" : 1}}]);

SET dialect='clickhouse';
DROP TABLE docs;
