drop table if exists tbl;
drop table if exists tbl2;

create table tbl(dt DateTime, i int, j String, v Float64) engine MergeTree partition by (toDate(dt), i % 2, length(j)) order by i settings index_granularity = 1;

insert into tbl values ('2021-04-01 00:01:02', 1, '123', 4), ('2021-04-01 01:01:02', 1, '12', 4), ('2021-04-01 02:11:02', 2, '345', 4), ('2021-04-01 04:31:02', 2, '2', 4), ('2021-04-02 00:01:02', 1, '1234', 4), ('2021-04-02 00:01:02', 2, '123', 4), ('2021-04-02 00:01:02', 3, '12', 4), ('2021-04-02 00:01:02', 4, '1', 4);

set optimize_use_implicit_projections = 1;

select count() from tbl where _partition_value = ('2021-04-01', 1, 2) settings max_rows_to_read = 1;
select count() from tbl where _partition_value.1 = '2021-04-01' settings max_rows_to_read = 4;
select count() from tbl where _partition_value.2 = 0 settings max_rows_to_read = 4;
select count() from tbl where _partition_value.3 = 4 settings max_rows_to_read = 1;

create table tbl2(i int) engine MergeTree order by i;
insert into tbl2 values (1);
select _partition_value from tbl2; -- { serverError UNKNOWN_IDENTIFIER }
select _partition_value from tbl2 group by 1; -- { serverError UNKNOWN_IDENTIFIER }

drop table tbl;
drop table tbl2;
