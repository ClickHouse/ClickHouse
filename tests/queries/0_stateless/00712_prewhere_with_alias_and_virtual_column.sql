drop table if exists tab_00712_1;
create table tab_00712_1 (a UInt32, b UInt32 alias a + 1, c UInt32) engine = MergeTree order by tuple();
insert into tab_00712_1 values (1, 2);
select ignore(_part) from tab_00712_1 prewhere b = 2;
drop table tab_00712_1;
