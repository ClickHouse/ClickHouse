-- `$set` keeps the existing document fields, including a preceding `$sort` key. An
-- order-sensitive `$group` after it must use that preserved order.
SET dialect = 'clickhouse';
DROP TABLE IF EXISTS group_order_set;
CREATE TABLE group_order_set (k String, ts UInt8, v String) ENGINE = MergeTree ORDER BY (k, ts);
INSERT INTO group_order_set VALUES ('a', 1, 'a1'), ('a', 2, 'a2'), ('b', 1, 'b1'), ('b', 2, 'b2');

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';
db.group_order_set.aggregate([{"$sort" : {"ts" : 1}}, {"$set" : {"v2" : "$v"}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v2"}}}, {"$sort" : {"_id" : 1}}]);

SET dialect = 'clickhouse';
DROP TABLE group_order_set;
