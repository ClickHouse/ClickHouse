-- Tags: no-s3-storage
drop table if exists t;

create table t (s UInt16, l UInt16, projection p (select s, l  order by l)) engine MergeTree order by s;

select s from t join (select toUInt16(1) as s) x using (s) settings allow_experimental_projection_optimization = 1;
select s from t join (select toUInt16(1) as s) x using (s) settings allow_experimental_projection_optimization = 0;

drop table t;
