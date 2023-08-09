drop table if exists tab;
create table tab (x LowCardinality(String)) engine = MergeTree order by tuple();

insert into tab values ('a'), ('bb'), ('a'), ('cc');

select count() as c, x in ('a', 'bb') as g from tab group by g order by c;

drop table if exists tab;
