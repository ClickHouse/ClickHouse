drop table if exists x;

create table x (i int, j int) engine MergeTree partition by i order by j settings index_granularity = 1;

insert into x values (1, 1), (1, 2), (1, 3), (2, 4), (2, 5), (2, 6);

set max_rows_to_read = 3;

select * from x where _partition_id = partitionId(1);

set max_rows_to_read = 4; -- one row for subquery

select * from x where _partition_id in (select partitionId(number + 1) from numbers(1));

-- trivial count optimization test
set max_rows_to_read = 1; -- one row for subquery
select count() from x where _partition_id in (select partitionId(number + 1) from numbers(1));

drop table x;

drop table if exists mt;

create table mt (n UInt64) engine=MergeTree order by n partition by n % 10;

set max_rows_to_read = 200;

insert into mt select * from numbers(100);

select * from mt where toUInt64(substr(_part, 1, position(_part, '_') - 1)) = 1;

drop table mt;
