-- An aggregation pipeline becomes a chain of `SELECT`s: a stage that needs a clause the current
-- select has already filled continues on top of a subquery, so the order of the stages is kept.
--
-- The queries below are, in order: a `$group` over the whole stream; `$match`, `$group`, `$sort`
-- and `$limit`; a document `_id`, which becomes one `_id.<field>` column per key; `$count`; a
-- `$match` after a `$group`, which filters the groups; `$project`; a `$set` that replaces a field;
-- `$skip` before `$limit`; `$limit` before `$skip`, which takes the first documents and only then
-- drops some of them and so is not a single `LIMIT ... OFFSET ...`; `$unionWith`, whose branches
-- may arrive in any order, so the `$group` over it uses the order-independent `$min` and `$max`
-- rather than `$push`; `$regexFind`,
-- which becomes the `match`, `idx` and `captures` fields of its result document; `$dateTrunc`;
-- `$cond`; a range on one field together with the Extended JSON a driver sends for a long; and
-- finally the stages and operators that are not supported, which have to be an error rather than
-- a silently wrong result.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS hits;
CREATE TABLE hits (CounterID Int32, RegionID Int32, UserID Int64, SearchPhrase String, URL String, ResolutionWidth Int32, EventTime DateTime) ENGINE = MergeTree ORDER BY CounterID;
INSERT INTO hits VALUES (1, 10, 100, 'hello', 'http://a.example.com/x', 1024, '2013-07-15 10:00:00'), (1, 10, 100, 'hello', 'http://a.example.com/y', 1280, '2013-07-15 10:01:00'), (2, 20, 200, 'world', 'http://b.example.com/z', 1920, '2013-07-16 11:00:00'), (2, 20, 201, '', 'http://b.example.com/w', 800, '2013-07-16 11:30:00');

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.hits.aggregate([{"$group" : {"_id" : null, "c" : {"$sum" : 1}, "w" : {"$sum" : "$ResolutionWidth"}, "a" : {"$avg" : "$ResolutionWidth"}}}]);

db.hits.aggregate([{"$match" : {"SearchPhrase" : {"$ne" : ""}}}, {"$group" : {"_id" : "$SearchPhrase", "c" : {"$sum" : 1}}}, {"$sort" : {"c" : -1, "_id" : 1}}, {"$limit" : 10}]);

db.hits.aggregate([{"$group" : {"_id" : {"RegionID" : "$RegionID", "UserID" : "$UserID"}}}, {"$group" : {"_id" : "$_id.RegionID", "u" : {"$sum" : 1}}}, {"$sort" : {"_id" : 1}}]);

db.hits.aggregate([{"$match" : {"CounterID" : 1}}, {"$count" : "c"}]);

db.hits.aggregate([{"$group" : {"_id" : "$CounterID", "c" : {"$sum" : 1}}}, {"$match" : {"c" : {"$gt" : 1}}}, {"$sort" : {"_id" : 1}}]);

db.hits.aggregate([{"$match" : {"CounterID" : 2}}, {"$project" : {"UserID" : 1, "double" : {"$multiply" : ["$ResolutionWidth", 2]}}}, {"$sort" : {"UserID" : 1}}]);

db.hits.aggregate([{"$group" : {"_id" : "$CounterID", "u" : {"$addToSet" : "$UserID"}}}, {"$set" : {"u" : {"$size" : "$u"}}}, {"$sort" : {"_id" : 1}}]);

db.hits.aggregate([{"$sort" : {"ResolutionWidth" : 1}}, {"$skip" : 1}, {"$limit" : 2}, {"$project" : {"ResolutionWidth" : 1}}]);

db.hits.aggregate([{"$sort" : {"ResolutionWidth" : 1}}, {"$limit" : 2}, {"$skip" : 1}, {"$project" : {"ResolutionWidth" : 1}}]);

db.hits.aggregate([{"$sort" : {"EventTime" : 1}}, {"$limit" : 1}, {"$unionWith" : {"coll" : "hits", "pipeline" : [{"$sort" : {"EventTime" : -1}}, {"$limit" : 1}]}}, {"$group" : {"_id" : null, "first" : {"$min" : "$EventTime"}, "last" : {"$max" : "$EventTime"}}}, {"$project" : {"first" : 1, "last" : 1}}]);

db.hits.aggregate([{"$set" : {"k" : {"$regexFind" : {"input" : "$URL", "regex" : "^https?://([^/]+)/"}}}}, {"$group" : {"_id" : {"$ifNull" : [{"$first" : "$k.captures"}, "$URL"]}, "c" : {"$sum" : 1}}}, {"$sort" : {"_id" : 1}}]);

db.hits.aggregate([{"$group" : {"_id" : {"$dateTrunc" : {"date" : "$EventTime", "unit" : "hour"}}, "c" : {"$sum" : 1}}}, {"$sort" : {"_id" : 1}}]);

db.hits.aggregate([{"$project" : {"kind" : {"$cond" : {"if" : {"$gte" : ["$ResolutionWidth", 1280]}, "then" : "wide", "else" : "narrow"}}}}, {"$group" : {"_id" : "$kind", "c" : {"$sum" : 1}}}, {"$sort" : {"_id" : 1}}]);

db.hits.aggregate([{"$match" : {"ResolutionWidth" : {"$gte" : 1024, "$lte" : 1280}, "UserID" : {"$numberLong" : "100"}}}, {"$count" : "c"}]);

SET dialect='clickhouse';
DROP TABLE hits;
