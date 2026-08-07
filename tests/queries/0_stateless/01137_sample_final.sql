drop table if exists tab;

create table tab (x UInt64, v UInt64) engine = ReplacingMergeTree(v) order by (x, sipHash64(x)) sample by sipHash64(x);
insert into tab select number, number from numbers(1000);
select * from tab final sample 1/2 order by x limit 5;

drop table tab;

create table tab (x UInt64, v UInt64) engine = ReplacingMergeTree(v) order by (x, sipHash64(x)) sample by sipHash64(x);
insert into tab select number, number from numbers(1000);
select sipHash64(x) from tab sample 1/2 order by x, sipHash64(x) limit 5;

drop table tab;
