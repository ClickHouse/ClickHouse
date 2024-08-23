drop table if exists test_native_parquet;
create table test_native_parquet (i16 Int16, i32 Int32, i64 Int64, float Float32, double Float64, string String, date Date32, time DateTime64) engine=File(Parquet) settings input_format_parquet_use_native_reader=true;
insert into test_native_parquet select number, number, number+1, 0.1*number, 0.2*number, toString(number), number, now64() + number from numbers(10000);

select * from test_native_parquet where i16 < 10;
select * from test_native_parquet where i16 <= 10;
select * from test_native_parquet where i16 between 10 and 20;
select * from test_native_parquet where i16 > 9990;
select * from test_native_parquet where i16 >= 9990;

select * from test_native_parquet where i32 < 10;
select * from test_native_parquet where i32 <= 10;
select * from test_native_parquet where i32 between 10 and 20;
select * from test_native_parquet where i32 > 9990;
select * from test_native_parquet where i32 >= 9990;

select * from test_native_parquet where i64 < 10;
select * from test_native_parquet where i64 <= 10;
select * from test_native_parquet where i64 between 10 and 20;
select * from test_native_parquet where i64 > 9990;
select * from test_native_parquet where i64 >= 9990;

select * from test_native_parquet where float < 1;
select * from test_native_parquet where float <= 1;
select * from test_native_parquet where float between 1 and 2;
select * from test_native_parquet where float > 999;
select * from test_native_parquet where float >= 999;

select * from test_native_parquet where double < 2;
select * from test_native_parquet where double <= 2;
select * from test_native_parquet where double between 2 and 4;
select * from test_native_parquet where double > 9990 *0.2;
select * from test_native_parquet where double >= 9990 *0.2;

select * from test_native_parquet where date < '1970-01-10';
select * from test_native_parquet where date <= '1970-01-10';
select * from test_native_parquet where date between '1970-01-10' and '1970-01-20';
select * from test_native_parquet where date > '1970-01-10';
select * from test_native_parquet where date >= '1970-01-10';

select * from test_native_parquet where string = '1';
select * from test_native_parquet where string in ('1','2','3');

drop table if exists test_native_parquet;
