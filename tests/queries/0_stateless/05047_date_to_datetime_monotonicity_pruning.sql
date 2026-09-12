-- Conversions of a `Date`/`Date32` day number to 32-bit seconds are monotonic only inside a
-- bounded window of day numbers, so key and statistics analysis must not assume order outside
-- it. `identity` hides the column from analysis and gives the unpruned answer.

set session_timezone = 'UTC'; -- these tests are timezone sensitive
set optimize_use_implicit_projections = 0;

-- { echoOn }
drop table if exists test;

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
-- The insert builds the statistics; the runner randomizes `materialize_statistics_on_insert` off.
insert into test_stat select toDate('2020-01-01') + number * 40, number from numbers(1300) SETTINGS materialize_statistics_on_insert = 1;
-- The statistics of this part are readable, so the two counts below compare a pruner that ran
-- against one that cannot. The probe is a range the part really excludes, since the wrapping
-- predicate is no longer allowed to prune and so can never show the entry.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stat WHERE d > toDate('2149-06-05')) WHERE explain ILIKE '%Statistics%';
SELECT count() FROM test_stat WHERE toDateTime32(d) > toDateTime('2020-11-06 00:00:00') SETTINGS use_statistics_for_part_pruning = 1;
SELECT count() FROM test_stat WHERE toDateTime32(identity(d)) > toDateTime('2020-11-06 00:00:00') SETTINGS use_statistics_for_part_pruning = 1;

-- A `Date32` day number above the window, and one above the window of the plain rescaling that
-- `toUnixTimestamp` applies.
drop table if exists test32;
create table test32 (d Date32, v Int64) engine MergeTree order by d settings index_granularity = 8, auto_statistics_types = '';
insert into test32 select toDate32('2000-01-01') + number * 400, number from numbers(210);
SELECT count() FROM test32 WHERE toDateTime32(d) > toDateTime('2050-01-01 00:00:00');
SELECT count() FROM test32 WHERE toDateTime32(identity(d)) > toDateTime('2050-01-01 00:00:00');
SELECT count() FROM test32 WHERE toUnixTimestamp(d) > 2524608000;
SELECT count() FROM test32 WHERE toUnixTimestamp(identity(d)) > 2524608000;

-- A `Date32` range that reaches day 0 wraps at the bottom in a timezone ahead of UTC, in every
-- overflow behaviour, because the day number is inspected before the timezone is applied. There
-- the false claim let an exactly continuous key range answer `count` without reading the rows,
-- which needs the implicit projection this file disabled above.
drop table if exists test32z;
create table test32z (d Date32, v Int64) engine MergeTree order by d settings auto_statistics_types = '';
insert into test32z select toDate32('1970-01-01') + number, number from numbers(40);
SELECT count() FROM test32z WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT count() FROM test32z WHERE toDateTime32(identity(d)) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT count() FROM test32z WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_projections = 1, optimize_use_implicit_projections = 1, date_time_overflow_behavior = 'saturate';
SELECT count() FROM test32z WHERE toDateTime32(identity(d)) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow', optimize_use_projections = 1, optimize_use_implicit_projections = 1, date_time_overflow_behavior = 'saturate';
-- An exactly continuous key range is searched by binary search, a range that is not by generic
-- exclusion; the raw-key arm is the control that shows the probe can print either value.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM test32z WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow') WHERE explain ILIKE '%generic exclusion search%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM test32z WHERE d > toDate32('1970-01-10')) WHERE explain ILIKE '%binary search%';

-- A `Date` day number reaches the saturated lookup table, which is never negative, so day 0 stays
-- inside the window and keeps pruning in the same timezone.
drop table if exists testz;
create table testz (d Date, v Int64) engine MergeTree order by d settings index_granularity = 8, auto_statistics_types = '';
insert into testz select toDate('1970-01-01') + number, number from numbers(40);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM testz WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow') WHERE explain ILIKE '%Granules: 4/5%';
SELECT count() FROM testz WHERE toDateTime32(d) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow';
SELECT count() FROM testz WHERE toDateTime32(identity(d)) > toDateTime('1970-01-10 12:00:00') SETTINGS session_timezone = 'Europe/Moscow';

-- The rescaling is reached through a wrapped source type too, where a rule about integer width
-- claims monotonicity for any day number. The range analysis strips `LowCardinality` but not
-- `Nullable`, so a `Nullable` source reaches that rule.
drop table if exists testn;
create table testn (d Nullable(Date), v Int64) engine MergeTree order by d settings allow_nullable_key = 1, auto_statistics_types = '';
insert into testn select toDate('2020-01-01') + number * 40, number from numbers(1300);
SELECT count() FROM testn WHERE toUnixTimestamp(d) > 1604620800;
SELECT count() FROM testn WHERE toUnixTimestamp(identity(d)) > 1604620800;

-- When the sorting key wraps the column, the constant of a predicate over the raw column is pushed
-- through the chain and compared against a wrapped key, so a part holding matching rows is skipped.
-- Merges stay off so each day band keeps its own part.
drop table if exists testk;
create table testk (d Date, v Int64) engine MergeTree order by toUnixTimestamp(d) settings auto_statistics_types = '';
system stop merges testk;
insert into testk select toDate('2020-01-01') + number, number from numbers(20);
insert into testk select toDate('2107-01-01') + number, number + 1000 from numbers(20);
SELECT count() FROM testk WHERE d > toDate('2020-11-06');
SELECT count() FROM testk WHERE identity(d) > toDate('2020-11-06');
SELECT count() FROM testk WHERE d > toDate('2020-11-06') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
