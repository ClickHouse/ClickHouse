drop table if exists projection_test;

create table projection_test (`sum(block_count)` UInt64, domain_alias UInt64 alias length(domain), datetime DateTime, domain LowCardinality(String), x_id String, y_id String, block_count Int64, retry_count Int64, duration Int64, kbytes Int64, buffer_time Int64, first_time Int64, total_bytes Nullable(UInt64), valid_bytes Nullable(UInt64), completed_bytes Nullable(UInt64), fixed_bytes Nullable(UInt64), force_bytes Nullable(UInt64), projection p (select toStartOfMinute(datetime) dt_m, countIf(first_time = 0) / count(), avg((kbytes * 8) / duration), count(), sum(block_count) / sum(duration), avg(block_count / duration), sum(buffer_time) / sum(duration), avg(buffer_time / duration), sum(valid_bytes) / sum(total_bytes), sum(completed_bytes) / sum(total_bytes), sum(fixed_bytes) / sum(total_bytes), sum(force_bytes) / sum(total_bytes), sum(valid_bytes) / sum(total_bytes), sum(retry_count) / sum(duration), avg(retry_count / duration), countIf(block_count > 0) / count(), countIf(first_time = 0) / count(), uniqHLL12(x_id), uniqHLL12(y_id) group by dt_m, domain)) engine MergeTree partition by toDate(datetime) order by (toStartOfTenMinutes(datetime), domain);

insert into projection_test with rowNumberInAllBlocks() as id select 1, toDateTime('2020-10-24 00:00:00') + (id / 20), toString(id % 100), * from generateRandom('x_id String, y_id String, block_count Int64, retry_count Int64, duration Int64, kbytes Int64, buffer_time Int64, first_time Int64, total_bytes Nullable(UInt64), valid_bytes Nullable(UInt64), completed_bytes Nullable(UInt64), fixed_bytes Nullable(UInt64), force_bytes Nullable(UInt64)', 10, 10, 1) limit 1000 settings max_threads = 1;

set allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

select * from projection_test; -- { serverError 584 }
select toStartOfMinute(datetime) dt_m, countIf(first_time = 0) from projection_test join (select 1) x using (1) where domain = '1' group by dt_m order by dt_m; -- { serverError 584 }

select toStartOfMinute(datetime) dt_m, countIf(first_time = 0) / count(), avg((kbytes * 8) / duration) from projection_test where domain = '1' group by dt_m order by dt_m;

-- prewhere with alias
select toStartOfMinute(datetime) dt_m, countIf(first_time = 0) / count(), avg((kbytes * 8) / duration) from projection_test prewhere domain_alias = 3 where domain = '1' group by dt_m order by dt_m;

drop row policy if exists filter on projection_test;
create row policy filter on projection_test using (domain = 'non_existing_domain') to all;
-- prewhere with alias with row policy (non existing)
select toStartOfMinute(datetime) dt_m, countIf(first_time = 0) / count(), avg((kbytes * 8) / duration) from projection_test prewhere domain_alias = 1 where domain = '1' group by dt_m order by dt_m;
drop row policy filter on projection_test;

-- TODO There is a bug in row policy filter (not related to projections, crash in master)
-- drop row policy if exists filter on projection_test;
-- create row policy filter on projection_test using (domain != '1') to all;
-- prewhere with alias with row policy (existing)
-- select toStartOfMinute(datetime) dt_m, countIf(first_time = 0) / count(), avg((kbytes * 8) / duration) from projection_test prewhere domain_alias = 1 where domain = '1' group by dt_m order by dt_m;
-- drop row policy filter on projection_test;

select toStartOfMinute(datetime) dt_m, count(), sum(block_count) / sum(duration), avg(block_count / duration) from projection_test group by dt_m order by dt_m;

-- TODO figure out how to deal with conflict column names
-- select toStartOfMinute(datetime) dt_m, count(), sum(block_count) / sum(duration), avg(block_count / duration) from projection_test where `sum(block_count)` = 1 group by dt_m order by dt_m;

select toStartOfMinute(datetime) dt_m, sum(buffer_time) / sum(duration), avg(buffer_time / duration), sum(valid_bytes) / sum(total_bytes), sum(completed_bytes) / sum(total_bytes), sum(fixed_bytes) / sum(total_bytes), sum(force_bytes) / sum(total_bytes), sum(valid_bytes) / sum(total_bytes) from projection_test where domain in ('12', '14') group by dt_m order by dt_m;

select toStartOfMinute(datetime) dt_m, domain, sum(retry_count) / sum(duration), avg(retry_count / duration), countIf(block_count > 0) / count(), countIf(first_time = 0) / count() from projection_test group by dt_m, domain having domain = '19' order by dt_m, domain;

select toStartOfHour(toStartOfMinute(datetime)) dt_h, uniqHLL12(x_id), uniqHLL12(y_id) from projection_test group by dt_h order by dt_h;

-- found by fuzzer
SELECT 2, -1 FROM projection_test PREWHERE domain_alias = 1. WHERE domain = NULL GROUP BY -9223372036854775808 ORDER BY countIf(first_time = 0) / count(-2147483649) DESC NULLS LAST, 1048576 DESC NULLS LAST;

drop table if exists projection_test;

drop table if exists projection_without_key;
create table projection_without_key (key UInt32, PROJECTION x (SELECT max(key))) engine MergeTree order by key;
insert into projection_without_key select number from numbers(1000);
set force_optimize_projection = 1, allow_experimental_projection_optimization = 1;
select max(key) from projection_without_key;
drop table projection_without_key;
