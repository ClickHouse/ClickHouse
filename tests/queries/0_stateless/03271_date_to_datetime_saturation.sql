drop table if exists test;

create table test (stamp Date) engine MergeTree order by stamp;

insert into test select '2024-10-30' from numbers(100);
insert into test select '2024-11-19' from numbers(100);
insert into test select '2149-06-06' from numbers(100);

optimize table test final;

-- { echoOn }
-- implicit toDateTime (always saturate)
select count() from test where stamp >= parseDateTimeBestEffort('2024-11-01');

select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'saturate';
select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'ignore';
select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

drop table test;

create table test (stamp Date) engine MergeTree order by stamp settings index_granularity = 20;

insert into test select number from numbers(65536);

set session_timezone = 'UTC'; -- The following tests are timezone sensitive
set optimize_use_implicit_projections = 0;

-- Boundary at UNIX epoch
SELECT count() FROM test WHERE stamp >= toDateTime(0) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(0);

-- Arbitrary DateTime
SELECT count() FROM test WHERE stamp >= toDateTime('2024-10-24 21:30:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('2024-10-24 21:30:00');

-- Extreme value beyond supported range
SELECT count() FROM test WHERE stamp >= toDateTime(4294967295) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(4294967295);

-- Negative timestamp
SELECT count() FROM test WHERE stamp >= toDateTime(-1) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(-1);

-- Pre-Gregorian date
SELECT count() FROM test WHERE stamp >= toDateTime('1000-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('1000-01-01 00:00:00');

-- UNIX epoch
SELECT count() FROM test WHERE stamp >= toDateTime('1970-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('1970-01-01 00:00:00');

-- Modern date within supported range
SELECT count() FROM test WHERE stamp >= toDateTime('2023-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('2023-01-01 00:00:00');

-- Far future but still valid
SELECT count() FROM test WHERE stamp >= toDateTime('2100-12-31 23:59:59') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('2100-12-31 23:59:59');

-- Maximum 32-bit timestamp
SELECT count() FROM test WHERE stamp >= toDateTime(2147483647) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(2147483647);

-- Maximum 32-bit unsigned overflow
SELECT count() FROM test WHERE stamp >= toDateTime(4294967295) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(4294967295);

-- Minimum Date boundary
SELECT count() FROM test WHERE stamp >= toDate('0000-01-01') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDate('0000-01-01');

-- Maximum Date boundary
SELECT count() FROM test WHERE stamp >= toDate('9999-12-31') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDate('9999-12-31');

-- Convert stamp to Date
SELECT count() FROM test WHERE toDate(stamp) >= toDateTime(0) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE toDate(identity(stamp)) >= toDateTime(0);

-- Convert stamp to DateTime (This will overflow and should not use primary key)
SELECT count() FROM test WHERE toDateTime(stamp) >= toDateTime(0) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM test WHERE toDateTime(identity(stamp)) >= toDateTime(0);

-- Exact Date match
SELECT count() FROM test WHERE stamp = toDate('2023-01-01') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) = toDate('2023-01-01');

-- Exact DateTime match
SELECT count() FROM test WHERE stamp = toDateTime('2023-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) = toDateTime('2023-01-01 00:00:00');

-- Invalid DateTime (negative)
SELECT count() FROM test WHERE stamp < toDateTime(-1) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) < toDateTime(-1);

-- Extremely large DateTime
SELECT count() FROM test WHERE stamp > toDateTime(9999999999) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) > toDateTime(9999999999);

-- NULL DateTime
SELECT count() FROM test WHERE stamp >= toDateTime(NULL) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(NULL);

-- NULL Date
SELECT count() FROM test WHERE stamp <= toDate(NULL) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) <= toDate(NULL);

-- Conversions of a Date/Date32 day number to 32-bit seconds are monotonic only inside a bounded
-- window of day numbers, so key, statistics and read-in-order analysis must not assume order
-- outside it. `identity` hides the column from analysis and gives the unpruned answer.
drop table test;

create table test (d Date, v Int64) engine MergeTree order by d settings auto_statistics_types = '';
insert into test select toDate('2020-01-01') + number * 40, number from numbers(1300);

SELECT count() FROM test WHERE toDateTime32(d) > toDateTime('2020-11-06 00:00:00');
SELECT count() FROM test WHERE toDateTime32(identity(d)) > toDateTime('2020-11-06 00:00:00');
SELECT count() FROM test WHERE CAST(d AS DateTime) > toDateTime('2020-11-06 00:00:00');
SELECT count() FROM test WHERE CAST(identity(d) AS DateTime) > toDateTime('2020-11-06 00:00:00');
SELECT count() FROM test WHERE accurateCast(d, 'DateTime') > toDateTime('2020-11-06 00:00:00');
SELECT count() FROM test WHERE accurateCast(identity(d), 'DateTime') > toDateTime('2020-11-06 00:00:00');
SELECT count() FROM test WHERE toUnixTimestamp(d) > 1604620800;
SELECT count() FROM test WHERE toUnixTimestamp(identity(d)) > 1604620800;

-- The same wrapping range on the statistics surface, which has no primary key to prune on.
drop table if exists test_stat;
create table test_stat (d Date STATISTICS(basic), v Int64) engine MergeTree order by tuple() settings auto_statistics_types = '';
insert into test_stat select toDate('2020-01-01') + number * 40, number from numbers(1300);
SELECT count() FROM test_stat WHERE toDateTime32(d) > toDateTime('2020-11-06 00:00:00');
SELECT count() FROM test_stat WHERE toDateTime32(identity(d)) > toDateTime('2020-11-06 00:00:00');

-- A Date32 day number above the window, and one above the window of the plain rescaling that
-- toUnixTimestamp applies.
drop table if exists test32;
create table test32 (d Date32, v Int64) engine MergeTree order by d settings index_granularity = 8, auto_statistics_types = '';
insert into test32 select toDate32('2000-01-01') + number * 400, number from numbers(210);
SELECT count() FROM test32 WHERE toDateTime32(d) > toDateTime('2050-01-01 00:00:00');
SELECT count() FROM test32 WHERE toDateTime32(identity(d)) > toDateTime('2050-01-01 00:00:00');
SELECT count() FROM test32 WHERE toUnixTimestamp(d) > 2524608000;
SELECT count() FROM test32 WHERE toUnixTimestamp(identity(d)) > 2524608000;

-- A Date32 range that reaches day 0 wraps at the bottom in a timezone ahead of UTC, in every
-- overflow behaviour, because the day number is inspected before the timezone is applied. There
-- the false claim let an exactly continuous key range answer count() without reading the rows,
-- which needs the implicit projection this file disabled above.
drop table if exists test32z;
create table test32z (d Date32, v Int64) engine MergeTree order by d settings auto_statistics_types = '';
insert into test32z select toDate32('1970-01-01') + number, number from numbers(40);
SELECT count() FROM test32z WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_implicit_projections = 1;
SELECT count() FROM test32z WHERE toDateTime32(identity(d)) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_implicit_projections = 1;
SELECT count() FROM test32z WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_implicit_projections = 1, date_time_overflow_behavior = 'saturate';
SELECT count() FROM test32z WHERE toDateTime32(identity(d)) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_implicit_projections = 1, date_time_overflow_behavior = 'saturate';

-- A Date day number reaches the saturated lookup table, which is never negative, so day 0 stays
-- inside the window and keeps pruning in the same timezone.
drop table if exists testz;
create table testz (d Date, v Int64) engine MergeTree order by d settings auto_statistics_types = '';
insert into testz select toDate('1970-01-01') + number, number from numbers(40);
SELECT count() FROM testz WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow';
SELECT count() FROM testz WHERE toDateTime32(identity(d)) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow';

-- Reading in the order of the sorting key requires order preservation over the whole column, which
-- these conversions do not have. The first row of an ascending order must be the minimum.
-- optimize_read_in_order is pinned because the test runner randomizes it.
drop table if exists testo;
create table testo (d Date) engine MergeTree order by d settings auto_statistics_types = '';
insert into testo select toDate('2100-01-01') + number * 500 from numbers(12);
SELECT (SELECT toDateTime(d) FROM testo ORDER BY toDateTime(d) LIMIT 1) = (SELECT min(toDateTime(d)) FROM testo) SETTINGS optimize_read_in_order = 1;
SELECT (SELECT toDateTime32(d) FROM testo ORDER BY toDateTime32(d) LIMIT 1) = (SELECT min(toDateTime32(d)) FROM testo) SETTINGS optimize_read_in_order = 1;
SELECT (SELECT CAST(d AS DateTime) FROM testo ORDER BY CAST(d AS DateTime) LIMIT 1) = (SELECT min(CAST(d AS DateTime)) FROM testo) SETTINGS optimize_read_in_order = 1;
SELECT (SELECT toUnixTimestamp(d) FROM testo ORDER BY toUnixTimestamp(d) LIMIT 1) = (SELECT min(toUnixTimestamp(d)) FROM testo) SETTINGS optimize_read_in_order = 1;
-- toUInt32 reads the day number unscaled, so it keeps order over the whole column.
SELECT (SELECT toUInt32(d) FROM testo ORDER BY toUInt32(d) LIMIT 1) = (SELECT min(toUInt32(d)) FROM testo) SETTINGS optimize_read_in_order = 1;

-- Day numbers wholly inside the window must still prune granules and still read in order.
drop table if exists testw;
create table testw (d Date, v Int64) engine MergeTree order by d settings index_granularity = 8, auto_statistics_types = '';
insert into testw select toDate('2000-01-01') + number * 10, number from numbers(392);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM testw WHERE toDateTime32(d) > toDateTime('2005-01-01 00:00:00')) WHERE explain ILIKE '%Granules: 27/49%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM testw WHERE toUnixTimestamp(d) > 1104537600) WHERE explain ILIKE '%Granules: 27/49%';
SELECT (SELECT toDateTime(d) FROM testw ORDER BY toDateTime(d) LIMIT 1) = (SELECT min(toDateTime(d)) FROM testw) SETTINGS optimize_read_in_order = 1;
