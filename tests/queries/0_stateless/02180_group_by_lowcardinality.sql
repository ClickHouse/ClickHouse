create table if not exists t_group_by_lowcardinality(p_date Date, val LowCardinality(Nullable(String))) 
engine=MergeTree() partition by p_date order by tuple();

insert into t_group_by_lowcardinality select today() as p_date, toString(number/5) as val from numbers(10000);
insert into t_group_by_lowcardinality select today() as p_date, Null as val from numbers(100);

select val, avg(toUInt32(val)) from t_group_by_lowcardinality group by val limit 10 settings max_threads=1, max_rows_to_group_by=100, group_by_overflow_mode='any' format JSONEachRow;

drop table if exists t_group_by_lowcardinality;

