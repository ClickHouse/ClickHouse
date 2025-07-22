-- { echo }
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 SECOND, 'US/Samoa');
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 MINUTE, 'US/Samoa');
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' HOUR, 'US/Samoa');
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 WEEK, 'US/Samoa');
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' MONTH, 'US/Samoa');
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' QUARTER, 'US/Samoa');
SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' YEAR, 'US/Samoa');

SELECT tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT tumbleStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT toDateTime(tumbleStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(tumbleStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT tumbleStart(tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'));
SELECT tumbleEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT toDateTime(tumbleEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(tumbleEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT tumbleEnd(tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'));

SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 SECOND, INTERVAL 3 SECOND, 'US/Samoa');
SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 MINUTE, INTERVAL 3 MINUTE, 'US/Samoa');
SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 HOUR, INTERVAL 3 HOUR, 'US/Samoa');
SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 DAY, INTERVAL 3 DAY, 'US/Samoa');
SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 WEEK, INTERVAL 3 WEEK, 'US/Samoa');
SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 MONTH, INTERVAL 3 MONTH, 'US/Samoa');
SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 QUARTER, INTERVAL 3 QUARTER, 'US/Samoa');
SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 YEAR, INTERVAL 3 YEAR, 'US/Samoa');

SELECT hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa');
SELECT hopStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa');
SELECT toDateTime(hopStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(hopStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT hopStart(hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'));
SELECT hopEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa');
SELECT toDateTime(hopEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(hopEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT hopEnd(hop(toDateTime('2019-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'));

SELECT hopStart(tuple()); -- { serverError ILLEGAL_COLUMN }
SELECT hopEnd(tuple()); -- { serverError ILLEGAL_COLUMN }
SELECT tumbleStart(tuple()); -- { serverError ILLEGAL_COLUMN }
SELECT tumbleEnd(tuple()); -- { serverError ILLEGAL_COLUMN }

SELECT tumbleStart(toUInt32(42)) SETTINGS session_timezone='UTC';
SELECT tumbleStart((now(), now(), 'meow')); -- { serverError ILLEGAL_COLUMN }
-- Check that it's not LOGICAL_ERROR.
create window view v to nonexist (x Int8) inner engine AggregatingMergeTree order by x as select x from nonexist group by tumble(now()) settings allow_experimental_window_view = 1, allow_experimental_analyzer = 0; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select hopEnd((makeDateTime(null), toDateTime('2025-02-07 17:23:42'))) SETTINGS session_timezone='UTC';
select hopStart((toDateTime('2025-02-07 17:23:42'), makeDateTime(null))) SETTINGS session_timezone='UTC';
select hopEnd((toDateTime('2025-02-07 17:23:42'), makeDateTime(null))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
