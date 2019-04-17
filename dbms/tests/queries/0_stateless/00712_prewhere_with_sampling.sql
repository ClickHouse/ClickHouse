drop table if exists tab;
create table tab (a UInt32, b UInt32) engine = MergeTree order by b % 2 sample by b % 2;
insert into tab values (1, 2), (1, 4);
select a from tab sample 1 / 2 prewhere b = 2;
drop table if exists tab;

DROP TABLE IF EXISTS sample_prewhere;
CREATE TABLE sample_prewhere (CounterID UInt32, UserID UInt64) ENGINE = MergeTree ORDER BY UserID SAMPLE BY UserID;
SELECT count() FROM sample_prewhere SAMPLE 1/2 PREWHERE CounterID = 1;
DROP TABLE sample_prewhere;
