-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on S3

-- Test that s3_create_new_file_on_insert setting works correctly when inserting via materialized view
-- See: https://github.com/ClickHouse/ClickHouse/issues/47263

-- { echo }
drop table if exists test_03700_mv;
drop table if exists test_03700_s3;
drop table if exists test_03700_source;

-- Create source MergeTree table
create table test_03700_source (a UInt64) engine = MergeTree() order by tuple();

-- Create S3 table with partitioning
create table test_03700_s3 (a UInt64) 
    engine = S3(s3_conn, filename='test_03700_{_partition_id}', format=Parquet) 
    partition by a;

-- Create materialized view from source to S3
create materialized view test_03700_mv to test_03700_s3 as select a from test_03700_source;

-- Set the truncate setting for initial insert
set s3_truncate_on_insert = 1;

-- Insert initial data through the MV
insert into test_03700_source values (1);
insert into test_03700_source values (2);

-- Now enable create_new_file_on_insert and disable truncate
set s3_truncate_on_insert = 0;
set s3_create_new_file_on_insert = 1;

-- Insert more data - this should create new files instead of failing
insert into test_03700_source values (1);
insert into test_03700_source values (2);

-- Verify we have all 4 values (2 original + 2 new)
select count() from s3(s3_conn, filename='test_03700_*', format=Parquet);

-- Verify the data content
select a from s3(s3_conn, filename='test_03700_*', format=Parquet) order by a;

-- Cleanup
drop table test_03700_mv;
drop table test_03700_s3;
drop table test_03700_source;
