drop table if exists test.lc;
create table test.lc (names Array(LowCardinality(String))) engine=MergeTree order by tuple();
insert into test.lc values ([]);
insert into test.lc select emptyArrayString();
select * from test.lc;
drop table if exists test.lc;

