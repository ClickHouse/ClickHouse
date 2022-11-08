-- Tags: no-s3-storage
drop table if exists t;

create table t (s UInt16, l UInt16, projection p (select s, l order by l)) engine MergeTree order by s;

select s from t join (select toUInt16(1) as s) x using (s) settings allow_experimental_projection_optimization = 1;
select s from t join (select toUInt16(1) as s) x using (s) settings allow_experimental_projection_optimization = 0;

drop table t;

drop table if exists mt;
create table mt (id1 Int8, id2 Int8) Engine=MergeTree order by tuple();
select id1 as alias1 from mt all inner join (select id2 as alias1 from mt) as t using (alias1) settings allow_experimental_projection_optimization = 1;
select id1 from mt all inner join (select id2 as id1 from mt) as t using (id1) settings allow_experimental_projection_optimization = 1;
select id2 as id1 from mt all inner join (select id1 from mt) as t using (id1) settings allow_experimental_projection_optimization = 1;
drop table mt;

drop table if exists j;
create table j (id1 Int8, id2 Int8, projection p (select id1, id2 order by id2)) Engine=MergeTree order by id1 settings index_granularity = 1;
insert into j select number, number from numbers(10);
select id1 as alias1 from j all inner join (select id2 as alias1 from j where id2 in (1, 2, 3)) as t using (alias1) where id2 in (2, 3, 4) settings allow_experimental_projection_optimization = 1;
drop table j;
