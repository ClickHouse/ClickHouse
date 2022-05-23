create table parquet_02312 (x Tuple(a UInt32, b UInt32)) engine=File(Parquet);
insert into parquet_02312 values ((1,2)), ((2,3)), ((3,4));
select * from parquet_02312;
drop table parquet_02312;
create table parquet_02312 (x Tuple(a UInt32, b UInt32)) engine=File(Arrow);
insert into parquet_02312 values ((1,2)), ((2,3)), ((3,4));
select * from parquet_02312;
drop table parquet_02312;
create table parquet_02312 (x Tuple(a UInt32, b UInt32)) engine=File(ORC);
insert into parquet_02312 values ((1,2)), ((2,3)), ((3,4));
select * from parquet_02312;
drop table parquet_02312;

