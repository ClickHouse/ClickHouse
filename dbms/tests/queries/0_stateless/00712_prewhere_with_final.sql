drop table if exists test.trepl;
create table test.trepl
(
d Date,
a Int32,
b Int32
) engine = ReplacingMergeTree(d, (a,b), 8192);


insert into test.trepl values ('2018-09-19', 1, 1);
select b from test.trepl FINAL prewhere a < 1000;
drop table test.trepl;

