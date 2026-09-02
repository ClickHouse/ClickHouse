drop table if exists test;
create table test (a Int8) engine = MergeTree order by tuple();
INSERT INTO test
SELECT x.number FROM (
 SELECT number
 FROM system.numbers
 LIMIT 10
) AS x
INNER JOIN input('a UInt64') AS y ON x.number = y.a
Format CSV 2

select * from test;
drop table test;

