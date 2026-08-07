-- month with 30 days
WITH
    toDate('2021-09-12') AS date_value,
    toDateTime('2021-09-12 11:22:33') AS date_time_value,
    toDateTime64('2021-09-12 11:22:33', 3) AS date_time_64_value
SELECT toLastDayOfMonth(date_value), toLastDayOfMonth(date_time_value), toLastDayOfMonth(date_time_64_value);

-- month with 31 days
WITH
    toDate('2021-03-12') AS date_value,
    toDateTime('2021-03-12 11:22:33') AS date_time_value,
    toDateTime64('2021-03-12 11:22:33', 3) AS date_time_64_value
SELECT toLastDayOfMonth(date_value), toLastDayOfMonth(date_time_value), toLastDayOfMonth(date_time_64_value);

-- non leap year February
WITH
    toDate('2021-02-12') AS date_value,
    toDateTime('2021-02-12 11:22:33') AS date_time_value,
    toDateTime64('2021-02-12 11:22:33', 3) AS date_time_64_value
SELECT toLastDayOfMonth(date_value), toLastDayOfMonth(date_time_value), toLastDayOfMonth(date_time_64_value);

-- leap year February
WITH
    toDate('2020-02-12') AS date_value,
    toDateTime('2020-02-12 11:22:33') AS date_time_value,
    toDateTime64('2020-02-12 11:22:33', 3) AS date_time_64_value
SELECT toLastDayOfMonth(date_value), toLastDayOfMonth(date_time_value), toLastDayOfMonth(date_time_64_value);

-- December 31 for non-leap year
WITH
    toDate('2021-12-12') AS date_value,
    toDateTime('2021-12-12 11:22:33') AS date_time_value,
    toDateTime64('2021-12-12 11:22:33', 3) AS date_time_64_value
SELECT toLastDayOfMonth(date_value), toLastDayOfMonth(date_time_value), toLastDayOfMonth(date_time_64_value);

-- December 31 for leap year
WITH
    toDate('2020-12-12') AS date_value,
    toDateTime('2020-12-12 11:22:33') AS date_time_value,
    toDateTime64('2020-12-12 11:22:33', 3) AS date_time_64_value
SELECT toLastDayOfMonth(date_value), toLastDayOfMonth(date_time_value), toLastDayOfMonth(date_time_64_value);

-- aliases
WITH
    toDate('2020-12-12') AS date_value
SELECT last_day(date_value), LAST_DAY(date_value);

-- boundaries
WITH
    toDate('1970-01-01') AS date_value,
    toDateTime('1970-01-01 11:22:33') AS date_time_value,
    toDateTime64('1900-01-01 11:22:33', 3) AS date_time_64_value
SELECT toLastDayOfMonth(date_value), toLastDayOfMonth(date_time_value), toLastDayOfMonth(date_time_64_value)
SETTINGS enable_extended_results_for_datetime_functions = true;

