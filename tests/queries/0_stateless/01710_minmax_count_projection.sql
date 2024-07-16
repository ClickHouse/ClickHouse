drop table if exists d;

create table d (i int, j int) engine MergeTree partition by i % 2 order by tuple() settings index_granularity = 1;

insert into d select number, number from numbers(10000);

set max_rows_to_read = 2, optimize_use_projections = 1, optimize_use_implicit_projections = 1;

select min(i), max(i), count() from d;
select min(i), max(i), count() from d group by _partition_id order by _partition_id;
select min(i), max(i), count() from d where _partition_value.1 = 0 group by _partition_id order by _partition_id;
select min(i), max(i), count() from d where moduloLegacy(i, 2) = 0 group by _partition_id order by _partition_id;
select min(i), max(i), count() from d where _partition_value.1 = 10 group by _partition_id order by _partition_id;

-- fuzz crash
select min(i) from d where 1 = _partition_value.1;

-- fuzz crash https://github.com/ClickHouse/ClickHouse/issues/37151
SELECT min(i), max(i), count() FROM d WHERE (_partition_value.1) = 0 GROUP BY ignore(bitTest(ignore(NULL), 0), NULL, (_partition_value.1) = 7, '10.25', bitTest(NULL, 0), NULL, ignore(ignore(-2147483647, NULL)), 1024), _partition_id ORDER BY _partition_id ASC NULLS FIRST;

drop table d;

drop table if exists has_final_mark;
drop table if exists mixed_final_mark;

create table has_final_mark (i int, j int) engine MergeTree partition by i % 2 order by j settings index_granularity = 10, write_final_mark = 1;
create table mixed_final_mark (i int, j int) engine MergeTree partition by i % 2 order by j settings index_granularity = 10;

set max_rows_to_read = 100000;

insert into has_final_mark select number, number from numbers(10000);

alter table mixed_final_mark attach partition 1 from has_final_mark;

set max_rows_to_read = 2;

select min(j) from has_final_mark;
select min(j) from mixed_final_mark;

select min(j), max(j) from has_final_mark;

set max_rows_to_read = 5001; -- one normal part 5000 + one minmax_count_projection part 1
select min(j), max(j) from mixed_final_mark;

-- The first primary expr is the same of some partition column
drop table if exists t;
create table t (server_date Date, something String) engine MergeTree partition by (toYYYYMM(server_date), server_date) order by (server_date, something);
insert into t values ('2019-01-01', 'test1'), ('2019-02-01', 'test2'), ('2019-03-01', 'test3');
select count() from t;
drop table t;

drop table if exists d;
create table d (dt DateTime, j int) engine MergeTree partition by (toDate(dt), ceiling(j), toDate(dt), CEILING(j)) order by tuple();
insert into d values ('2021-10-24 10:00:00', 10), ('2021-10-25 10:00:00', 10), ('2021-10-26 10:00:00', 10), ('2021-10-27 10:00:00', 10);
select min(dt), max(dt), count() from d where toDate(dt) >= '2021-10-25';
-- fuzz crash
select min(dt), max(dt), count(toDate(dt) >= '2021-10-25') from d where toDate(dt) >= '2021-10-25';
select count() from d group by toDate(dt);

-- fuzz crash
SELECT min(dt), count(ignore(ignore(ignore(tupleElement(_partition_value, 'xxxx', NULL) = NULL), NULL, NULL, NULL), 0, '10485.76', NULL)), max(dt), count(toDate(dt) >= '2021-10-25') FROM d WHERE toDate(dt) >= '2021-10-25';

-- fuzz crash
SELECT pointInEllipses(min(j), NULL), max(dt), count('0.0000000007') FROM d WHERE toDate(dt) >= '2021-10-25';
SELECT min(j) FROM d PREWHERE ceil(j) <= 0;
SELECT min(dt) FROM d PREWHERE ((0.9998999834060669 AND 1023) AND 255) <= ceil(j);
SELECT count('') AND NULL FROM d PREWHERE ceil(j) <= NULL;

drop table d;

-- count variant optimization

drop table if exists test;
create table test (id Int64, d Int64, projection dummy(select * order by id)) engine MergeTree order by id;
insert into test select number, number from numbers(1e3);

select count(if(d=4, d, 1)) from test settings force_optimize_projection = 1;
select count(d/3) from test settings force_optimize_projection = 1;
select count(if(d=4, Null, 1)) from test settings force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

drop table test;
