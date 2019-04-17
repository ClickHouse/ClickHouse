drop table if exists lc;
create table lc (names Array(LowCardinality(String))) engine=MergeTree order by tuple();
insert into lc values ([]);
insert into lc select emptyArrayString();
select * from lc;
drop table if exists lc;

