drop table if exists test.tab;
create table test.tab (a UInt32, b UInt32) engine = MergeTree order by b % 2 sample by b % 2;
insert into test.tab values (1, 2), (1, 4);
select a from test.tab sample 1 / 2 prewhere b = 2;
drop table if exists test.tab;

