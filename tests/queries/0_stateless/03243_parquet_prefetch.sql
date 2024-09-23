-- Tags: no-fasttest, no-parallel

insert into table function hdfs('hdfs://localhost:12222/test_03243.parquet', 'Parquet') select number as i from numbers(100000) settings output_format_parquet_row_group_size=10000,hdfs_truncate_on_insert=1;
select max(i) from table function hdfs('hdfs://localhost:12222/test_03243.parquet', 'Parquet');

