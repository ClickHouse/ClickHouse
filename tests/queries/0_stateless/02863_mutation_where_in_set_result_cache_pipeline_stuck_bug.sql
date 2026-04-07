drop table if exists tab;
create table tab (x UInt32, y UInt32) engine = MergeTree order by x;

insert into tab select number, number from numbers(10);
insert into tab select number, number from numbers(20);

set mutations_sync=2;

alter table tab delete where x > 1000 and y in (select sum(number + 1) from numbers_mt(1e7) group by number % 2 with totals);
drop table if exists tab;
