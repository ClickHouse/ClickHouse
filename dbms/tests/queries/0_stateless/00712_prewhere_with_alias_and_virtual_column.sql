drop table if exists tab;
create table tab (a UInt32, b UInt32 alias a + 1, c UInt32) engine = MergeTree order by tuple();
insert into tab values (1, 2);
select ignore(_part) from tab prewhere b = 2;

