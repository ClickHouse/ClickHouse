-- The pipeline stages beyond the ones a ClickBench query needs, following what SingleStore Kai
-- supports: `$unset`, `$sortByCount`, `$sample`, `$unwind` and `$replaceRoot`/`$replaceWith`.
--
-- The queries below are, in order: `$unset` of one field and of several; `$sortByCount`;
-- `$sample`, counted rather than listed because it picks at random; `$unwind`, which is an
-- `ARRAY JOIN` and so drops a document whose array is empty unless asked to keep it - and a
-- document kept that way answers with no element rather than with the default value of one - with
-- and without the index of the element; a `$match` before an `$unwind`, which filters the documents
-- and not the elements; a `$match` after one, which filters the elements; `$replaceRoot` and
-- `$replaceWith`; and the stages that are not supported.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS events;
CREATE TABLE events (id Int32, kind String, tags Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO events VALUES (1, 'click', ['red', 'green']), (2, 'view', ['green']), (3, 'click', []), (4, 'click', ['blue']);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.events.aggregate([{"$unset" : "tags"}, {"$sort" : {"id" : 1}}]);
db.events.aggregate([{"$unset" : ["tags", "kind"]}, {"$sort" : {"id" : 1}}]);

db.events.aggregate([{"$sortByCount" : "$kind"}]);

db.events.aggregate([{"$sample" : {"size" : 2}}, {"$count" : "c"}]);

db.events.aggregate([{"$unwind" : "$tags"}, {"$sort" : {"id" : 1, "tags" : 1}}]);
db.events.aggregate([{"$unwind" : {"path" : "$tags", "preserveNullAndEmptyArrays" : true}}, {"$sort" : {"id" : 1, "tags" : 1}}]);
db.events.aggregate([{"$unwind" : {"path" : "$tags", "includeArrayIndex" : "position"}}, {"$sort" : {"id" : 1, "position" : 1}}]);

db.events.aggregate([{"$match" : {"kind" : "click"}}, {"$unwind" : "$tags"}, {"$sort" : {"id" : 1, "tags" : 1}}]);
db.events.aggregate([{"$unwind" : "$tags"}, {"$match" : {"tags" : "green"}}, {"$sort" : {"id" : 1}}]);
db.events.aggregate([{"$unwind" : "$tags"}, {"$sortByCount" : "$tags"}]);

db.events.aggregate([{"$replaceRoot" : {"newRoot" : {"key" : "$id", "upper" : {"$toUpper" : "$kind"}}}}, {"$sort" : {"key" : 1}}]);
db.events.aggregate([{"$replaceWith" : {"key" : "$id"}}, {"$sort" : {"key" : 1}}]);

SET dialect='clickhouse';
DROP TABLE events;
