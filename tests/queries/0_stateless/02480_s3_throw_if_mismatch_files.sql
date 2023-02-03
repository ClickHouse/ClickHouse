-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop table if exists test_02480_mismatch_files;
create table test_02480_mismatch_files (a UInt64, b String) engine = S3(s3_conn, filename='test_02480_mismatch_files_{_partition_id}', format=Parquet) partition by a;
set s3_truncate_on_insert=1;
insert into test_02480_mismatch_files values (1, 'a'), (22, 'b'), (333, 'c');

select a, b from s3(s3_conn, filename='test_02480_mismatch_filesxxx*', format=Parquet);  -- { serverError 636 }

select a, b from s3(s3_conn, filename='test_02480_mismatch_filesxxx*', format=Parquet) settings s3_allow_throw_if_mismatch_files=1;  -- { serverError 107 }
