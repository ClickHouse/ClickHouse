select '-- test without MergeTree table';
select autoregress(x-> toFloat64(x+1.25), 1, toFloat64(100)) from numbers(20);

select '-- test with MergeTree table';
drop table if exists auto_regress_0001;
create table auto_regress_0001(id UInt64, col1 Float64, col2 Int64) engine=MergeTree() order by id;
insert into auto_regress_0001(id, col1, col2) select number, number * 1.5, number * 10 from numbers(10);

select '-- backward offset is 1, initial value is constant';
select autoregress(x-> toFloat64(col1 * col2 + x), 1, toFloat64(0)), col1, col2, col1 * col2 from auto_regress_0001 order by id asc;

select '-- backward offset is 2, initial value is constant';
select autoregress(x-> toFloat64(col1 * col2 + x), 2, toFloat64(0)), col1, col2, col1 * col2 from auto_regress_0001 order by id asc;

select '-- backward offset is 1, initial value is an expression';
select autoregress(x-> toFloat64(col1 * col2 + x), 1, toFloat64(col1+101)), col1, col2, col1 * col2, col1+101 from auto_regress_0001 order by id asc;

select '-- backward offset is 2, initial value is an expression';
select autoregress(x-> toFloat64(col1 * col2 + x), 2, toFloat64(col1+101.5)), col1, col2, col1 * col2, col1+101.5 from auto_regress_0001 order by id asc limit 9;

select '-- backward offset is 3, and limit only one row';
select autoregress(x-> toFloat64(col1 * col2 + x), 3, toFloat64(0)), col1, col2, col1 * col2 from auto_regress_0001 order by id asc limit 1;

drop table auto_regress_0001;
