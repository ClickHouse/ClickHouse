SET dialect='clickhouse';

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    c0 Int32,
    c1 Int32,
    c2 String,
)
ENGINE = Memory;
INSERT INTO test(c0, c1, c2) VALUES (1, 2, 'ac');
INSERT INTO test(c0, c1, c2) VALUES (4, 5, 'b');
INSERT INTO test(c0, c1, c2) VALUES (7, 3, 'efaaaa');
INSERT INTO test(c0, c1, c2) VALUES (8, 6, 'gh');
INSERT INTO test(c0, c1, c2) VALUES (2, 9, 'ij');
INSERT INTO test(c0, c1, c2) VALUES (3, 4, 'kl');
INSERT INTO test(c0, c1, c2) VALUES (5, 7, 'mn');
INSERT INTO test(c0, c1, c2) VALUES (9, 8, 'op');
INSERT INTO test(c0, c1, c2) VALUES (6, 1, 'qr');
INSERT INTO test(c0, c1, c2) VALUES (10, 12, 'st');
INSERT INTO test(c0, c1, c2) VALUES (11, 14, 'uv');
INSERT INTO test(c0, c1, c2) VALUES (13, 15, 'wx');
INSERT INTO test(c0, c1, c2) VALUES (16, 11, 'yz');
INSERT INTO test(c0, c1, c2) VALUES (14, 13, 'ab');

SET dialect='mongo';
db.test.find({});
db.test.find({"c0" : 1});
db.test.find({"c0" : 2});
db.test.find({"c0" : 1, "c1" : 2});
db.test.find({"$or" : [{"c0" : 1}, {"c0" : 3}]});
db.test.find({"c0" : 1, "c1" : {"$lt" : 3}});
db.test.find({"c0" : 1, "c1" : {"$lte" : 3}});
db.test.find({"c0" : 1, "c1" : {"$gt" : 1}});
db.test.find({"c0" : 1, "c1" : {"$gte" : 2}});
db.test.find({"c0" : 1, "c1" : {"$ne" : 0}});
db.test.find({"c2" : {"$regex" : "%a%"}});
db.test.find({}).limit(1);
db.test.find({}).sort({"c0" : 1});
db.test.find({"$projection" : {"b0" : "c0"}});
db.test.find({"$projection" : {"b0" : "c0", "b1" : {"$add" : ["c0", "c1"]}}});
db.test.find({"$projection" : {"b0" : "c0", "b1" : {"$mul" : ["c0", "c1"]}}});
db.test.find({"$projection" : {"b0" : "c0", "b1" : {"$div" : ["c0", "c1"]}}});
db.test.find({"$projection" : {"b0" : "c0", "b1" : {"$add" : ["c0", {"$mul" : ["c0", "c1"]}]}}});
