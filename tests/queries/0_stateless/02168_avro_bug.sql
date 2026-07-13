-- Tags: no-fasttest, no-parallel
insert into table function file('02168_avro_bug.avro', 'Parquet', 'x UInt64') select * from numbers(10) settings engine_file_truncate_on_insert=1;
insert into table function file('02168_avro_bug.avro', 'Parquet', 'x UInt64') select * from numbers(10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into table function file('02168_avro_bug.avro', 'Parquet', 'x UInt64') select * from numbers(10); -- { serverError CANNOT_APPEND_TO_FILE }
select 'OK';
