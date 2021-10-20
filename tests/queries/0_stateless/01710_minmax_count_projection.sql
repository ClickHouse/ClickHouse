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

drop table if exists no_final_mark;
drop table if exists has_final_mark;
drop table if exists mixed_final_mark;

create table no_final_mark (i int, j int) engine MergeTree partition by i % 2 order by j settings index_granularity = 10, write_final_mark = 0;
create table has_final_mark (i int, j int) engine MergeTree partition by i % 2 order by j settings index_granularity = 10, write_final_mark = 1;
create table mixed_final_mark (i int, j int) engine MergeTree partition by i % 2 order by j settings index_granularity = 10;

set max_rows_to_read = 100000;

insert into no_final_mark select number, number from numbers(10000);
insert into has_final_mark select number, number from numbers(10000);

alter table mixed_final_mark attach partition 0 from no_final_mark;
alter table mixed_final_mark attach partition 1 from has_final_mark;

set max_rows_to_read = 2;

select min(j) from no_final_mark;
select min(j) from has_final_mark;
select min(j) from mixed_final_mark;

select min(j), max(j) from no_final_mark; -- {serverError TOO_MANY_ROWS}
select min(j), max(j) from has_final_mark;

set max_rows_to_read = 5001; -- one normal part 5000 + one minmax_count_projection part 1
select min(j), max(j) from mixed_final_mark;
