drop table if exists t;

create table t(id UInt32) engine MergeTree order by id;

insert into t values (1);

select count() from t array join range(2) as a settings optimize_trivial_count_query = 1;

drop table t;
