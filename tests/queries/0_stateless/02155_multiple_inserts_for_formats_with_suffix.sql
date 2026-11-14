-- Tags: no-fasttest, no-parallel

drop table if exists test;
create table test (number UInt64) engine=File('Parquet');
insert into test select * from numbers(10);
insert into test select * from numbers(10, 10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into test select * from numbers(10, 10) settings engine_file_allow_create_multiple_files=1;
select * from test order by number;
truncate table test;
drop table test;

create table test (number UInt64) engine=File('Parquet', 'test_02155/test1/data.Parquet');
insert into test select * from numbers(10) settings engine_file_truncate_on_insert=1;
insert into test select * from numbers(10, 10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into test select * from numbers(10, 10) settings engine_file_allow_create_multiple_files=1;
select * from test order by number;
drop table test;


insert into table function file(concat(currentDatabase(), '/test2/data.Parquet'), 'Parquet', 'number UInt64') select * from numbers(10) settings engine_file_truncate_on_insert=1;
insert into table function file(concat(currentDatabase(), '/test2/data.Parquet'), 'Parquet', 'number UInt64') select * from numbers(10, 10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into table function file(concat(currentDatabase(), '/test2/data.Parquet'), 'Parquet', 'number UInt64') select * from numbers(10, 10) settings engine_file_allow_create_multiple_files=1;
select * from file(concat(currentDatabase(), '/test2/data.Parquet'), 'Parquet', 'number UInt64');
select * from file(concat(currentDatabase(), '/test2/data.1.Parquet'), 'Parquet', 'number UInt64');

create table test (number UInt64) engine=File('Parquet', 'test_02155/test3/data.Parquet.gz');
insert into test select * from numbers(10) settings engine_file_truncate_on_insert=1;
;
insert into test select * from numbers(10, 10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into test select * from numbers(10, 10) settings engine_file_allow_create_multiple_files=1;
select * from test order by number;
drop table test;

insert into table function file(concat(currentDatabase(), '/test4/data.Parquet.gz'), 'Parquet', 'number UInt64') select * from numbers(10) settings engine_file_truncate_on_insert=1;
insert into table function file(concat(currentDatabase(), '/test4/data.Parquet.gz'), 'Parquet', 'number UInt64') select * from numbers(10, 10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into table function file(concat(currentDatabase(), '/test4/data.Parquet.gz'), 'Parquet', 'number UInt64') select * from numbers(10, 10) settings engine_file_allow_create_multiple_files=1;
select * from file(concat(currentDatabase(), '/test4/data.Parquet.gz'), 'Parquet', 'number UInt64');
select * from file(concat(currentDatabase(), '/test4/data.1.Parquet.gz'), 'Parquet', 'number UInt64');

