-- Tags: no-fasttest
insert into table function file('data.avro', 'Avro', 'x UInt64') select * from numbers(10);
insert into table function file('data.avro', 'Avro', 'x UInt64') select * from numbers(10);
insert into table function file('data.avro', 'Avro', 'x UInt64') select * from numbers(10);
select 'OK';
