-- Mongo defines the value of the accumulators `$first`, `$last`, `$push`, `$firstN` and `$lastN`
-- by the order of the documents of the group, so a `$sort` right before a `$group` is how a
-- pipeline asks for the earliest or the latest document of each key. A ClickHouse aggregate
-- function reads its input in whatever order the query produces it in, so those accumulators are
-- lowered through the keys of the `$sort` instead.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS group_order;
CREATE TABLE group_order (k String, ts Int64, v String) ENGINE = MergeTree ORDER BY (k, ts);
INSERT INTO group_order VALUES ('a', 1, 'a1'), ('a', 2, 'a2'), ('a', 3, 'a3'), ('b', 1, 'b1'), ('b', 2, 'b2');

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.group_order.aggregate([{"$sort": {"ts": 1}}, {"$group": {"_id": "$k", "first": {"$first": "$v"}, "last": {"$last": "$v"}, "all": {"$push": "$v"}}}, {"$sort": {"_id": 1}}]);

db.group_order.aggregate([{"$sort": {"ts": -1}}, {"$group": {"_id": "$k", "first": {"$first": "$v"}, "last": {"$last": "$v"}, "all": {"$push": "$v"}}}, {"$sort": {"_id": 1}}]);

db.group_order.aggregate([{"$sort": {"ts": 1}}, {"$group": {"_id": "$k", "firstTwo": {"$firstN": {"input": "$v", "n": 2}}, "lastTwo": {"$lastN": {"input": "$v", "n": 2}}}}, {"$sort": {"_id": 1}}]);

db.group_order.aggregate([{"$sort": {"k": 1, "ts": 1}}, {"$group": {"_id": null, "first": {"$first": "$v"}, "last": {"$last": "$v"}}}]);

SET dialect='clickhouse';
DROP TABLE group_order;
