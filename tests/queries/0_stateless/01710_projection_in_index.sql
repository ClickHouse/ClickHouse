-- Tags: no-s3-storage
drop table if exists t;

create table t (i int, j int, k int, projection p (select * order by j)) engine MergeTree order by i settings index_granularity = 1;

insert into t select number, number, number from numbers(10);

set allow_experimental_projection_optimization = 1, max_rows_to_read = 3;

select * from t where i < 5 and j in (1, 2);

drop table t;
