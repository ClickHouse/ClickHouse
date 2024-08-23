drop table if exists int_native_parquet;
drop table if exists int_arrow_parquet;
drop table if exists int_mergetree;

create table int_native_parquet (i1 Int64, i2 Int64, i3 Int64, i4 Int64, i5 Int64, i6 Int64, i7 Int64) engine=File(Parquet) settings input_format_parquet_use_native_reader=true;
create table int_arrow_parquet (i1 Int64, i2 Int64, i3 Int64, i4 Int64, i5 Int64, i6 Int64, i7 Int64) engine=File(Parquet) settings input_format_parquet_use_native_reader=false;
create table int_mergetree (i1 Int64, i2 Int64, i3 Int64, i4 Int64, i5 Int64, i6 Int64, i7 Int64) engine=MergeTree() order by tuple();


insert into int_native_parquet select (number%100000)/10, rand()%100, rand()%1000, rand(), rand(), rand(), rand() from numbers(100000000);
insert into int_arrow_parquet select (number%100000)/10, rand()%100, rand()%1000, rand(), rand(), rand(), rand() from numbers(100000000);
insert into int_mergetree select (number%100000)/10, rand()%100, rand()%1000, rand(), rand(), rand(), rand() from numbers(100000000);
optimize table int_mergetree;

select * from int_native_parquet where i1 < 10 Format Null; # 0.399s
select * from int_arrow_parquet where i1 < 10 Format Null;  # 1.207s
select * from int_mergetree where i1 < 10 Format Null;      # 0.095s

select * from int_native_parquet where i2 < 10 Format Null; # 0.717s
select * from int_arrow_parquet where i2 < 10 Format Null;  # 1.358s
select * from int_mergetree where i2 < 10 Format Null;      # 0.658s

select * from int_native_parquet where i3 < 10 Format Null; # 0.670s
select * from int_arrow_parquet where i3 < 10 Format Null;  # 1.245s
select * from int_mergetree where i3 < 10 Format Null;  # 0.663s

drop table if exists int_native_parquet;
drop table if exists int_arrow_parquet;
drop table if exists int_mergetree;


drop table if exists string_native_parquet;
drop table if exists string_arrow_parquet;
drop table if exists string_mergetree;

create table string_native_parquet (s1 String, s2 String, s3 String, s4 String, s5 String, s6 String, s7 String) engine=File(Parquet) settings input_format_parquet_use_native_reader=true;
create table string_arrow_parquet (s1 String, s2 String, s3 String, s4 String, s5 String, s6 String, s7 String) engine=File(Parquet) settings input_format_parquet_use_native_reader=false;
create table string_mergetree (s1 String, s2 String, s3 String, s4 String, s5 String, s6 String, s7 String) engine=MergeTree() order by tuple();

insert into string_native_parquet select toString((number%100000)/10), toString(rand()%100), toString(rand()%1000), toString(rand()), toString(rand()), toString(rand()), toString(rand()) from numbers(100000000);
insert into string_arrow_parquet select toString((number%100000)/10), toString(rand()%100), toString(rand()%1000), toString(rand()), toString(rand()), toString(rand()), toString(rand()) from numbers(100000000);
insert into string_mergetree select toString((number%100000)/10), toString(rand()%100), toString(rand()%1000), toString(rand()), toString(rand()), toString(rand()), toString(rand()) from numbers(100000000);
optimize table string_mergetree final;

select * from string_native_parquet where s1 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10') Format Null;  # 0.598s
select * from string_arrow_parquet where s1 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null;  # 2.694s
select * from string_mergetree where s1 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null;      # 0.187s

select * from string_native_parquet where s2 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null; # 1.228s
select * from string_arrow_parquet where s2 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null;  # 3.006s
select * from string_mergetree where s2 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null;      # 1.502s

select * from string_native_parquet where s3 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null; # 1.140s
select * from string_arrow_parquet where s3 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null;  # 2.707s
select * from string_mergetree where s3 in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')  Format Null;      # 1.310s

drop table if exists string_native_parquet;
drop table if exists string_native_parquet;
drop table if exists string_mergetree;
