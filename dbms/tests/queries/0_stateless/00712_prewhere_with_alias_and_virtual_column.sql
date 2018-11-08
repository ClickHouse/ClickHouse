drop table if exists test.tab;
create table test.tab (a UInt32, b UInt32 alias a + 1, c UInt32) engine = MergeTree order by tuple();
insert into test.tab values (1, 2);
select ignore(_part) from test.tab prewhere b = 2;

