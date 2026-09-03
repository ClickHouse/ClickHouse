-- Tags: no-fasttest

insert into table function file('/dev/null', 'Parquet', 'number UInt64') select * from numbers(10);
insert into table function file('/dev/null', 'ORC', 'number UInt64') select * from numbers(10);
insert into table function file('/dev/null', 'JSON', 'number UInt64') select * from numbers(10);

