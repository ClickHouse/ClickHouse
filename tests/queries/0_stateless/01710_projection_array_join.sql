-- Tags: no-s3-storage
set allow_experimental_projection_optimization = 1;

drop table if exists x;

create table x (pk int, arr Array(int), projection p (select arr order by pk)) engine MergeTree order by tuple();

insert into x values (1, [2]);

select a from x array join arr as a;

drop table x;
