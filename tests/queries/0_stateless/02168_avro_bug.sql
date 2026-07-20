-- Tags: no-fasttest, no-parallel
insert into table function file('data.avro', 'Parquet', 'x UInt64') select * from numbers(10);
insert into table function file('data.avro', 'Parquet', 'x UInt64') select * from numbers(10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into table function file('data.avro', 'Parquet', 'x UInt64') select * from numbers(10); -- { serverError CANNOT_APPEND_TO_FILE }
select 'OK';
