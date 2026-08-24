drop table if exists test;
create table test(day Date, id UInt32) engine=MergeTree partition by day order by tuple();
insert into test select toDate('2023-01-05') AS day, number from numbers(10);
with toUInt64(id) as id_with select day, count(id_with) from test where day >= '2023-01-01' group by day limit 1000;
drop table test;
