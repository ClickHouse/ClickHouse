drop table if exists tab;
create table tab (x UInt32) engine = MergeTree order by tuple();

insert into tab select number from numbers(10);

set alter_sync = 0;
alter table tab update x = x + sleepEachRow(0.1) where 1;
alter table tab modify column x String;
alter table tab add column y String default x || '_42';

select x, y from tab order by x;
