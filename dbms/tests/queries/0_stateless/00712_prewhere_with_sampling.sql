drop table if exists test.tab;
create table test.tab (a UInt32, b UInt32) engine = MergeTree order by b % 2 sample by b % 2;
insert into test.tab values (1, 2), (1, 4);
select a from test.tab sample 1 / 2 prewhere b = 2;
drop table if exists test.tab;

DROP TABLE IF EXISTS test.sample_prewhere;
CREATE TABLE test.sample_prewhere (CounterID UInt32, UserID UInt64) ENGINE = MergeTree ORDER BY UserID SAMPLE BY UserID;
SELECT count() FROM test.sample_prewhere SAMPLE 1/2 PREWHERE CounterID = 1;
DROP TABLE test.sample_prewhere;
