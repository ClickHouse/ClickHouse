drop table if exists test_native_parquet;
create table test_native_parquet (a Int32, b Int64, c Float32, d Float64, s String, dat Date32, time DateTime64) engine=File(Parquet) settings input_format_parquet_use_native_reader=true;
insert into test_native_parquet select number, number+1, 0.1*number, 0.2*number, toString(number), number, now64() + number from numbers(10000);

select * from test_native_parquet where a < 10;
select * from test_native_parquet where a <= 10;
select * from test_native_parquet where a between 10 and 20;
select * from test_native_parquet where a > 9990;
select * from test_native_parquet where a >= 9990;

select * from test_native_parquet where b < 10;
select * from test_native_parquet where b <= 10;
select * from test_native_parquet where b between 10 and 20;
select * from test_native_parquet where b > 9990;
select * from test_native_parquet where b >= 9990;

select * from test_native_parquet where c < 1;
select * from test_native_parquet where c <= 1;
select * from test_native_parquet where c between 1 and 2;
select * from test_native_parquet where c > 999;
select * from test_native_parquet where c >= 999;

select * from test_native_parquet where b < 2;
select * from test_native_parquet where b <= 2;
select * from test_native_parquet where b between 2 and 4;
select * from test_native_parquet where b > 9990 *0.2;
select * from test_native_parquet where b >= 9990 *0.2;

select * from test_native_parquet where s = '1';
select * from test_native_parquet where s in ('1','2','3');

drop table if exists test_native_parquet;
