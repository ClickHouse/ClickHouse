-- Tags: no-fasttest

-- { echoOn }
-- support data types: Int16, Int32, Int64, Float32, Float64, String, Date32, DateTime64, Date, DateTime
drop table if exists test_native_parquet;
create table test_native_parquet (i16 Int16, i32 Int32, i64 Int64, float Float32, double Float64, string String, date32 Date32, time64 DateTime64, date Date, time DateTime) engine=File(Parquet) settings input_format_parquet_use_native_reader_with_filter_push_down=true;
insert into test_native_parquet select number, number, number+1, 0.1*number, 0.2*number, toString(number), number, toDateTime('2024-10-11 00:00:00') + number, number, toDateTime('2024-10-11 00:00:00') + number from numbers(10000);
-- test int16
select sum(i16) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where i16 < 10;
select count(), sum(cityHash64(*)) from test_native_parquet where i16 <= 10;
select count(), sum(cityHash64(*)) from test_native_parquet where i16 between 10 and 20;
select count(), sum(cityHash64(*)) from test_native_parquet where i16 > 9990;
select count(), sum(cityHash64(*)) from test_native_parquet where i16 >= 9990;

-- test int32
select sum(i32) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where i32 < 10;
select count(), sum(cityHash64(*)) from test_native_parquet where i32 <= 10;
select count(), sum(cityHash64(*)) from test_native_parquet where i32 between 10 and 20;
select count(), sum(cityHash64(*)) from test_native_parquet where i32 > 9990;
select count(), sum(cityHash64(*)) from test_native_parquet where i32 >= 9990;

-- test int64
select sum(i64) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where i64 < 10;
select count(), sum(cityHash64(*)) from test_native_parquet where i64 <= 10;
select count(), sum(cityHash64(*)) from test_native_parquet where i64 between 10 and 20;
select count(), sum(cityHash64(*)) from test_native_parquet where i64 > 9990;
select count(), sum(cityHash64(*)) from test_native_parquet where i64 >= 9990;

-- test float
select sum(float) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where float < 1;
select count(), sum(cityHash64(*)) from test_native_parquet where float <= 1;
select count(), sum(cityHash64(*)) from test_native_parquet where float between 1 and 2;
select count(), sum(cityHash64(*)) from test_native_parquet where float > 999;
select count(), sum(cityHash64(*)) from test_native_parquet where float >= 999;

-- test double
select sum(double) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where double < 2;
select count(), sum(cityHash64(*)) from test_native_parquet where double <= 2;
select count(), sum(cityHash64(*)) from test_native_parquet where double between 2 and 4;
select count(), sum(cityHash64(*)) from test_native_parquet where double > 9990 *0.2;
select count(), sum(cityHash64(*)) from test_native_parquet where double >= 9990 *0.2;

-- test date
select max(date32) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where date32 < '1970-01-10';
select count(), sum(cityHash64(*)) from test_native_parquet where date32 <= '1970-01-10';
select count(), sum(cityHash64(*)) from test_native_parquet where date32 between '1970-01-10' and '1970-01-20';
select count(), sum(cityHash64(*)) from test_native_parquet where date32 > '1970-01-10';
select count(), sum(cityHash64(*)) from test_native_parquet where date32 >= '1970-01-10';

-- test datetime
select max(time64) from test_native_parquet;

-- test String
select max(string) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where string = '1';
select count(), sum(cityHash64(*)) from test_native_parquet where string in ('1','2','3');

-- test date
select max(date) from test_native_parquet;
select count(), sum(cityHash64(*)) from test_native_parquet where date < '1970-01-10';
select count(), sum(cityHash64(*)) from test_native_parquet where date <= '1970-01-10';
select count(), sum(cityHash64(*)) from test_native_parquet where date between '1970-01-10' and '1970-01-20';
select count(), sum(cityHash64(*)) from test_native_parquet where date > '1970-01-10';
select count(), sum(cityHash64(*)) from test_native_parquet where date >= '1970-01-10';

-- test datetime
select max(time) from test_native_parquet;

drop table if exists test_nullable_native_parquet;
create table test_nullable_native_parquet (i16 Nullable(Int16), i32 Nullable(Int32), i64 Nullable(Int64), float Nullable(Float32), double Nullable(Float64), string Nullable(String), date32 Nullable(Date32), time64 Nullable(DateTime64), date Nullable(Date), time Nullable(DateTime)) engine=File(Parquet) settings input_format_parquet_use_native_reader_with_filter_push_down=true;
insert into test_nullable_native_parquet select if(number%5, number, NULL), if(number%5, number, NULL), if(number%5, number+1, NULL), if(number%5, 0.1*number, NULL), if(number%5, 0.2*number, NULL), toString(number), if(number%5, number, NULL), if(number%5, toDateTime('2024-10-11 00:00:00') + number, NULL), if(number%5, number, NULL), if(number%5, toDateTime('2024-10-11 00:00:00') + number, NULL) from numbers(10000);
-- test int16
select sum(i16) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i16 < 10;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i16 <= 10;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i16 between 10 and 20;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i16 > 9990;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i16 >= 9990;

-- test int32
select sum(i32) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i32 < 10;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i32 <= 10;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i32 between 10 and 20;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i32 > 9990;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i32 >= 9990;

-- test int64
select sum(i64) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i64 < 10;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i64 <= 10;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i64 between 10 and 20;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i64 > 9990;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where i64 >= 9990;

-- test float
select sum(float) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where float < 1;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where float <= 1;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where float between 1 and 2;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where float > 999;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where float >= 999;

-- test double
select sum(double) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where double < 2;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where double <= 2;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where double between 2 and 4;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where double > 9990 *0.2;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where double >= 9990 *0.2;

-- test date
select max(date32) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date32 < '1970-01-10';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date32 <= '1970-01-10';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date32 between '1970-01-10' and '1970-01-20';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date32 > '1970-01-10';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date32 >= '1970-01-10';

-- test datetime
select max(time64) from test_nullable_native_parquet;

-- test String
select max(string) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where string = '1';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where string in ('1','2','3');

-- test date
select max(date) from test_nullable_native_parquet;
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date < '1970-01-10';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date <= '1970-01-10';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date between '1970-01-10' and '1970-01-20';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date > '1970-01-10';
select count(), sum(cityHash64(*)) from test_nullable_native_parquet where date >= '1970-01-10';

-- test datetime
select max(time) from test_nullable_native_parquet;
