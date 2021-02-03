DROP TABLE IF EXISTS union1;
DROP TABLE IF EXISTS union2;
CREATE TABLE union1 ( date Date, a Int32, b Int32, c Int32, d Int32) ENGINE = MergeTree(date, (a, date), 8192);
CREATE TABLE union2 ( date Date, a Int32, b Int32, c Int32, d Int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'union1');
ALTER TABLE union2 MODIFY ORDER BY a; -- { serverError 48 }
DROP TABLE union1;
DROP TABLE union2;
