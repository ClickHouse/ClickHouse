drop table if exists test;
create table test
(
   n1 UInt32,
   n2 UInt32 alias murmurHash3_32(n1),
   n3 UInt32 materialized n2 + 1
)engine=MergeTree order by n1;
insert into test select * from generateRandom() limit 10;
drop table test;
