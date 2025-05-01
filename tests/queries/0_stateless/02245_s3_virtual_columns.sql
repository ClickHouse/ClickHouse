-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop table if exists test_02245;
create table test_02245 (a UInt64) engine = S3(s3_conn, filename='test_02245', format=Parquet);
insert into test_02245 select 1 settings s3_truncate_on_insert=1;
select * from test_02245;
select _path from test_02245;

drop table if exists test_02245_2;
create table test_02245_2 (a UInt64, _path Int32) engine = S3(s3_conn, filename='test_02245_2', format=Parquet);
insert into test_02245_2 select 1, 2 settings s3_truncate_on_insert=1;
select * from test_02245_2;
select _path, isNotNull(_etag) from test_02245_2;
select count() from test_02245_2 where _etag IN (select _etag from test_02245_2);
