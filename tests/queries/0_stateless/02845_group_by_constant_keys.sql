select count(number), 1 AS k1, 2 as k2, 3 as k3 from numbers_mt(10000000) group by k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=0;
select count(number), 1 AS k1, 2 as k2, 3 as k3 from numbers_mt(10000000) group by k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions = 0;
select count(number), 1 AS k1, 2 as k2, 3 as k3 from numbers_mt(10000000) group by k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions = 1;
select count(number), 1 AS k1, 2 as k2, 3 as k3 from numbers_mt(10000000) group by k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions = 1;

drop table if exists test;
create table test (x UInt64) engine=File(JSON);
set engine_file_allow_create_multiple_files = 1;
insert into test select * from numbers(10);
insert into test select * from numbers(10);
insert into test select * from numbers(10);

select count() from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=0;
select count() from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions=0;
select count() from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=1;
select count() from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions=1;

select count(), _file from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=0;
select count(), _file from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions=0;
select count(), _file from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=1;
select count(), _file from test group by _file order by _file settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions=1;

select count() from test group by _file, _path order by _file, _path settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=0;
select count() from test group by _file, _path order by _file, _path settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions=0;
select count() from test group by _file, _path order by _file, _path settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=1;
select count() from test group by _file, _path order by _file, _path settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions=1;

drop table test;
