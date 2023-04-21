-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop table if exists test_02480_support_wildcard_write;
drop table if exists test_02480_support_wildcard_write2;
create table test_02480_support_wildcard_write (a UInt64, b String) engine = S3(s3_conn, filename='test_02480_support_wildcard_{_partition_id}', format=Parquet) partition by a;
set s3_truncate_on_insert=1;
insert into test_02480_support_wildcard_write values (1, 'a'), (22, 'b'), (333, 'c');

select a, b from s3(s3_conn, filename='test_02480_support_wildcard_*', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_support_wildcard_?', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_support_wildcard_??', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_support_wildcard_?*?', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_support_wildcard_{1,333}', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_support_wildcard_{1..333}', format=Parquet) order by a;

create table test_02480_support_wildcard_write2 (a UInt64, b String) engine = S3(s3_conn, filename='prefix/test_02480_support_wildcard_{_partition_id}', format=Parquet) partition by a;
set s3_truncate_on_insert=1;
insert into test_02480_support_wildcard_write2 values (4, 'd'), (55, 'f'), (666, 'g');

select a, b from s3(s3_conn, filename='*/test_02480_support_wildcard_*', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='*/test_02480_support_wildcard_?', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='prefix/test_02480_support_wildcard_??', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='prefi?/test_02480_support_wildcard_*', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='p?*/test_02480_support_wildcard_{56..666}', format=Parquet) order by a;

drop table test_02480_support_wildcard_write;
drop table test_02480_support_wildcard_write2;
