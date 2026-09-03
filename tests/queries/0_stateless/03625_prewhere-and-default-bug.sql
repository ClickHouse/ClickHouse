drop table if exists tab;
create table tab (d DateTime64(3), p LowCardinality(String)) engine = MergeTree order by toDate(d);
insert into tab select toDateTime(toDate('2000-01-01')) + number, if(bitAnd(number, 1) = 0, 'a', 'b') from numbers(100);

alter table tab add column t String default '' settings alter_sync = 2;

select 1 from tab where d > toDateTime(toDate('2000-01-01')) and p in ('a') and 1 = 1 group by d, t, p;
