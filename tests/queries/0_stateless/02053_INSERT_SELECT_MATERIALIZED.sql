-- Test from https://github.com/ClickHouse/ClickHouse/issues/29729
create table data_02053 (id Int64, A Nullable(Int64), X Int64 materialized coalesce(A, -1)) engine=MergeTree order by id;
insert into data_02053 values (1, 42);
-- Due to insert_null_as_default A became Null and X became -1
insert into data_02053 select 1, 42;
select *, X from data_02053 order by id;
