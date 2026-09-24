-- A `$project` carries a preceding `$sort` key into the documents it builds when it keeps that
-- field as itself - an inclusion that names it, or an exclusion that does not remove it - so an
-- order-sensitive `$group` after it uses that order. A projection that drops the key or replaces
-- it with an expression is rejected instead of answering in an arbitrary order; that case is
-- covered by `04852_mongo_dialect_group_order_rejections`.
SET dialect = 'clickhouse';
DROP TABLE IF EXISTS group_order_project;
CREATE TABLE group_order_project (k String, ts UInt8, v String) ENGINE = MergeTree ORDER BY (k, ts);
INSERT INTO group_order_project VALUES ('a', 1, 'a1'), ('a', 2, 'a2'), ('b', 1, 'b1'), ('b', 2, 'b2');

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';
db.group_order_project.aggregate([{"$sort" : {"ts" : 1}}, {"$project" : {"k" : 1, "ts" : 1, "v" : 1}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}, "all" : {"$push" : "$v"}}}, {"$sort" : {"_id" : 1}}]);
db.group_order_project.aggregate([{"$sort" : {"ts" : -1}}, {"$project" : {"v" : 0}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$ts"}}}, {"$sort" : {"_id" : 1}}]);

SET dialect = 'clickhouse';
DROP TABLE group_order_project;
