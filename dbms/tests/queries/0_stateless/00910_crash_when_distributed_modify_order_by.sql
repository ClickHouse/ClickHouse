DROP TABLE IF EXISTS test.union1;
DROP TABLE IF EXISTS test.union2;
CREATE TABLE test.union1 ( date Date, a Int32, b Int32, c Int32, d Int32) ENGINE = MergeTree(date, (a, date), 8192);
CREATE TABLE test.union2 ( date Date, a Int32, b Int32, c Int32, d Int32) ENGINE = Distributed(test_shard_localhost, 'test', 'union1');
ALTER TABLE test.union2 MODIFY ORDER BY a; -- { serverError 48 }
DROP TABLE test.union1;
DROP TABLE test.union2;
