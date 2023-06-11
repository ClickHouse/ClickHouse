drop table if exists t;

create table t (i int, j int) engine MergeTree order by i;

insert into t values (1, 2);

alter table t add projection x (select * order by j);

insert into t values (1, 4);

set allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

select i from t prewhere j = 4;

SELECT j = 2, i FROM t PREWHERE j = 2;

drop table t;
