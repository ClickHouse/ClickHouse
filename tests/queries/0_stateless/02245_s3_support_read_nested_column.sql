-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop table if exists test_02245_s3_nested_parquet1;
drop table if exists test_02245_s3_nested_parquet2;
set input_format_parquet_import_nested = 1;
set s3_truncate_on_insert = 1;
create table test_02245_s3_nested_parquet1(a Int64, b Tuple(a Int64, b String)) engine=S3(s3_conn, filename='test_02245_s3_nested_parquet1_{_partition_id}', format='Parquet') partition by a;
insert into test_02245_s3_nested_parquet1 values (1, (2, 'a'));

select a, b.a, b.b from s3(s3_conn, filename='test_02245_s3_nested_parquet1_*', format='Parquet');

create table test_02245_s3_nested_parquet2(a Int64, b Tuple(a Int64, b Tuple(c Int64, d String))) engine=S3(s3_conn, filename='test_02245_s3_nested_parquet2_{_partition_id}', format='Parquet') partition by a;
insert into test_02245_s3_nested_parquet2 values (1, (2, (3, 'a')));

select a, b.a, b.b.c, b.b.d from s3(s3_conn, filename='test_02245_s3_nested_parquet2_*', format='Parquet', structure='a Int64, b Tuple(a Int64, b Tuple(c Int64, d String))');


drop table if exists test_02245_s3_nested_arrow1;
drop table if exists test_02245_s3_nested_arrow2;
set input_format_arrow_import_nested=1;
create table test_02245_s3_nested_arrow1(a Int64, b Tuple(a Int64, b String)) engine=S3(s3_conn, filename='test_02245_s3_nested_arrow1_{_partition_id}', format='Arrow') partition by a;
insert into test_02245_s3_nested_arrow1 values (1, (2, 'a'));

select a, b.a, b.b from s3(s3_conn, filename='test_02245_s3_nested_arrow1_*', format='Arrow');

create table test_02245_s3_nested_arrow2(a Int64, b Tuple(a Int64, b Tuple(c Int64, d String))) engine=S3(s3_conn, filename='test_02245_s3_nested_arrow2_{_partition_id}', format='Arrow') partition by a;
insert into test_02245_s3_nested_arrow2 values (1, (2, (3, 'a')));

select a, b.a, b.b.c, b.b.d from s3(s3_conn, filename='test_02245_s3_nested_arrow2_*', format='Arrow', structure='a Int64, b Tuple(a Int64, b Tuple(c Int64, d String))');


drop table if exists test_02245_s3_nested_orc1;
drop table if exists test_02245_s3_nested_orc2;
set input_format_orc_import_nested=1;
create table test_02245_s3_nested_orc1(a Int64, b Tuple(a Int64, b String)) engine=S3(s3_conn, filename='test_02245_s3_nested_orc1_{_partition_id}', format='ORC') partition by a;
insert into test_02245_s3_nested_orc1 values (1, (2, 'a'));

select a, b.a, b.b from s3(s3_conn, filename='test_02245_s3_nested_orc1_*', format='ORC');

create table test_02245_s3_nested_orc2(a Int64, b Tuple(a Int64, b Tuple(c Int64, d String))) engine=S3(s3_conn, filename='test_02245_s3_nested_orc2_{_partition_id}', format='ORC') partition by a;
insert into test_02245_s3_nested_orc2 values (1, (2, (3, 'a')));

select a, b.a, b.b.c, b.b.d from s3(s3_conn, filename='test_02245_s3_nested_orc2_*', format='ORC', structure='a Int64, b Tuple(a Int64, b Tuple(c Int64, d String))');
