drop table if exists test_uint64;
create table test_uint64 (`data` UInt64 Default 0) engine = MergeTree order by tuple();
insert into test_uint64 values ('0'), (NULL), (1), ('2');
drop table if exists test_uint64;

drop table if exists test_float64;
create table test_float64 (`data` Float64 Default 0.0) engine = MergeTree order by tuple();
insert into test_float64 values ('0.1'), (NULL), (1.1), ('2.2');
drop table if exists test_float64;

drop table if exists test_date;
create table test_date (`data` Date) engine = MergeTree order by tuple();
insert into test_date values ('2021-01-01'), (NULL), ('2021-02-01'), ('2021-03-01');
drop table if exists test_date;

drop table if exists test_datetime;
create table test_datetime (`data` DateTime) engine = MergeTree order by tuple();
insert into test_datetime values ('2021-01-01 00:00:00'), (NULL), ('2021-02-01 01:00:00'), ('2021-03-01 02:00:00');
drop table if exists test_datetime;
