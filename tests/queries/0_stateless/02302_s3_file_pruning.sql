-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop table if exists test_02302;
create table test_02302 (a UInt64) engine = S3(s3_conn, filename='test_02302_{_partition_id}', format=Parquet) partition by a;
insert into test_02302 select number from numbers(10) settings s3_truncate_on_insert=1;
select * from test_02302 order by a;
select * from test_02302 where _file like '%1';
select * from test_02302 where a = '1';

drop table if exists test_02302_another;
create table test_02302_another (a UInt64) engine = S3(s3_conn, filename='test_02302_{_partition_id}_another', format=Parquet) partition by a;
insert into test_02302_another select number from numbers(10) settings s3_truncate_on_insert=1;
select _file, * from test_02302_another order by _file;
select _file, * from test_02302 order by _file;
select _file, * from test_02302_another where a = '1' order by _file;
select _file, * from test_02302 where a = '1' order by _file;
drop table test_02302;
drop table test_02302_another;

set max_rows_to_read = 1;

-- Test s3 table function with glob
select * from s3(s3_conn, filename='test_02302_*', format=Parquet) where _file like '%5';

-- Test s3 table with explicit keys (no glob)
-- TODO support truncate table function
drop table if exists test_02302;
create table test_02302 (a UInt64) engine = S3(s3_conn, filename='test_02302.2', format=Parquet);
truncate table test_02302;

drop table if exists test_02302;
create table test_02302 (a UInt64) engine = S3(s3_conn, filename='test_02302.1', format=Parquet);
truncate table test_02302;

drop table if exists test_02302;
create table test_02302 (a UInt64) engine = S3(s3_conn, filename='test_02302', format=Parquet);
truncate table test_02302;

insert into test_02302 select 0 settings s3_create_new_file_on_insert = true;
insert into test_02302 select 1 settings s3_create_new_file_on_insert = true;
insert into test_02302 select 2 settings s3_create_new_file_on_insert = true;

select * from test_02302 where _file like '%1';
drop table test_02302;
