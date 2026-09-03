drop table if exists projection_without_key;

create table projection_without_key (key UInt32, PROJECTION x (SELECT sum(key) group by key % 3)) engine MergeTree order by key;
insert into projection_without_key select number from numbers(1000);
select sum(key) from projection_without_key settings optimize_use_projections = 1;
select sum(key) from projection_without_key settings optimize_use_projections = 0;

drop table projection_without_key;
