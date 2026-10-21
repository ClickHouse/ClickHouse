-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop table if exists test_04004_hash_write;
drop table if exists test_04004_hash_write2;
drop table if exists test_04004_hash_partitioned;

-- Test 1: Basic write and read via table with {_schema_hash}
create table test_04004_hash_write (a UInt64, b String) engine = S3(s3_conn, filename='test_04004/{_schema_hash}/data.parquet', format=Parquet);
set s3_truncate_on_insert=1;
insert into test_04004_hash_write values (1, 'hello'), (2, 'world');
select a, b from test_04004_hash_write order by a;

-- Test 2: Different schema, same base path pattern — writes to a different directory due to different hash
create table test_04004_hash_write2 (x Float64) engine = S3(s3_conn, filename='test_04004/{_schema_hash}/data.parquet', format=Parquet);
insert into test_04004_hash_write2 values (3.14), (2.72);
select round(x, 2) from test_04004_hash_write2 order by x;

-- Verify first table still reads only its own data (not cross-contaminated)
select a, b from test_04004_hash_write order by a;

-- Test 3: Combined {_schema_hash} and {_partition_id} — write via table, read via s3() glob
create table test_04004_hash_partitioned (a UInt64, b String) engine = S3(s3_conn, filename='test_04004/{_schema_hash}/{_partition_id}/data.parquet', format=Parquet) partition by a;
insert into test_04004_hash_partitioned values (1, 'foo'), (2, 'bar'), (3, 'baz');
select a, b from s3(s3_conn, filename='test_04004/*/*/data.parquet', format=Parquet) order by a;

-- Test 4: Error when using {_schema_hash} without specifying columns
create table test_04004_hash_no_schema engine = S3(s3_conn, filename='test_04004/{_schema_hash}/data.parquet', format=Parquet); -- { serverError BAD_ARGUMENTS }

-- Test 5: Error when combining {_schema_hash} with hive partition strategy
create table test_04004_hash_hive (a UInt64, b String) engine = S3(s3_conn, filename='test_04004/{_schema_hash}/data.parquet', format=Parquet, partition_strategy='hive') partition by a; -- { serverError BAD_ARGUMENTS }

-- Test 6: Truncate should work with {_schema_hash}
truncate table test_04004_hash_write;
select count() from test_04004_hash_write settings s3_ignore_file_doesnt_exist=1;

-- Test 7: Truncate should fail for combined {_schema_hash} + {_partition_id}
truncate table test_04004_hash_partitioned; -- { serverError NOT_IMPLEMENTED }

drop table test_04004_hash_write;
drop table test_04004_hash_write2;
drop table test_04004_hash_partitioned;
