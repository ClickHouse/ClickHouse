
-- https://github.com/yandex/ClickHouse/issues/1059

DROP TABLE IF EXISTS test.union1;
DROP TABLE IF EXISTS test.union2;
DROP TABLE IF EXISTS test.union3;

CREATE TABLE test.union1 ( date Date, a Int32, b Int32, c Int32, d Int32) ENGINE = MergeTree(date, (a, date), 8192);
CREATE TABLE test.union2 ( date Date, a Int32, b Int32, c Int32, d Int32) ENGINE = Distributed(test_shard_localhost, 'test', 'union1');
CREATE TABLE test.union3 ( date Date, a Int32, b Int32, c Int32, d Int32) ENGINE = Distributed(test_shard_localhost, 'test', 'union2');

INSERT INTO test.union1 VALUES (1,  2, 3, 4, 5);
INSERT INTO test.union1 VALUES (11,12,13,14,15);
INSERT INTO test.union2 VALUES (21,22,23,24,25);
INSERT INTO test.union3 VALUES (31,32,33,34,35);

select b, sum(c) from ( select a, b, sum(c) as c from test.union2 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from test.union2 where b>1 group by a, b ) as a group by b;
select b, sum(c) from ( select a, b, sum(c) as c from test.union1 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from test.union2 where b>1 group by a, b ) as a group by b;
select b, sum(c) from ( select a, b, sum(c) as c from test.union1 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from test.union1 where b>1 group by a, b ) as a group by b;
select b, sum(c) from ( select a, b, sum(c) as c from test.union2 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from test.union3 where b>1 group by a, b ) as a group by b;

DROP TABLE test.union1;
DROP TABLE test.union2;
DROP TABLE test.union3;
