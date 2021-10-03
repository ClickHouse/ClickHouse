drop table if exists d;

create table d (i int, j int) engine MergeTree partition by i % 2 order by tuple() settings index_granularity = 1;

insert into d select number, number from numbers(10000);

set max_rows_to_read = 2, allow_experimental_projection_optimization = 1;

select min(i), max(i), count() from d;
select min(i), max(i), count() from d group by _partition_id order by _partition_id;
select min(i), max(i), count() from d where _partition_value.1 = 0 group by _partition_id order by _partition_id;
select min(i), max(i), count() from d where _partition_value.1 = 10 group by _partition_id order by _partition_id;

-- fuzz crash
select min(i) from d where 1 = _partition_value.1;

drop table d;
