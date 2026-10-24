-- From the function documentation examples
-- Daily step (default)
SELECT generate_date_array('2024-01-01', '2024-01-05') AS example;
-- Monthly step
SELECT generate_date_array('2024-03-07', '2024-08-07', INTERVAL 1 MONTH) AS example;
-- Descending (negative step, wraps over the month boundary and the leap day)
SELECT generate_date_array('2024-03-02', '2024-02-27', INTERVAL -1 DAY) AS example;
-- Same start and end — single-element array
SELECT generate_date_array('2024-03-15', '2024-03-15') AS example;
-- end before start with positive step — empty array
SELECT generate_date_array('2024-01-05', '2024-01-01') AS example;
-- Mixed Date32 and Date with single element
SELECT generate_date_array('2024-03-15'::Date32, '2024-03-15'::Date, INTERVAL 1 DAY) AS example;
-- Subquery
SELECT generate_date_array(date_start, date_end) AS date_range
FROM (SELECT toDate('2024-01-01') AS date_start, toDate('2024-01-07') AS date_end
      UNION ALL SELECT toDate('2024-02-15') AS date_start, toDate('2024-02-21') AS date_end
) ORDER BY date_range;

-- Taken from Big Query docs and extended
-- Default step of 1 day
SELECT generate_date_array('2016-10-05'::Date, '2016-10-08'::Date) AS example;
-- User-specified step of 2 days
SELECT generate_date_array( '2016-10-05'::Date, '2016-10-09'::Date, INTERVAL 2 DAY) AS example;
-- Negative step of 3 days
SELECT generate_date_array('2016-10-05'::Date, '2016-10-01'::Date, INTERVAL -3 DAY) AS example;
-- Same value for start and end day
SELECT generate_date_array('2016-10-05'::Date, '2016-10-05'::Date, INTERVAL 8 DAY) AS example;
SELECT generate_date_array('2016-10-05'::Date, '2016-10-05'::Date, INTERVAL -1 DAY) AS example;
-- End date before start date with positive step results in empty array
SELECT generate_date_array('2016-10-05'::Date, '2016-10-01'::Date, INTERVAL 1 DAY) AS example;
-- If one input is NULL, the result is NULL.
SELECT generate_date_array('2016-10-05'::Date, NULL) AS example;
SELECT generate_date_array(NULL, '2016-10-06'::Date) AS example;
SELECT generate_date_array(NULL, NULL) AS example;
SELECT generate_date_array('2016-10-05'::Date, '2016-10-06'::Date, INTERVAL NULL DAY) AS example;
-- Uses MONTH as interval
SELECT generate_date_array('2016-01-01'::Date, '2016-12-31'::Date, INTERVAL 2 MONTH) AS example;
-- Non-constant start/end dates
SELECT generate_date_array(date_start, date_end, INTERVAL 1 WEEK) AS date_range
FROM (
  SELECT DATE '2024-01-16' AS date_start, DATE '2024-03-31' AS date_end
  UNION ALL SELECT DATE '2024-04-05', DATE '2024-04-30'
  UNION ALL SELECT DATE '2024-07-29', DATE '2024-09-02'
  UNION ALL SELECT DATE '2024-10-01', DATE '2024-10-31'
) AS items ORDER BY date_range;

-- String inputs (no explicit cast required)
-- Both strings, defaults to Array(Date)
SELECT generate_date_array('2016-10-05', '2016-10-08') AS example;
-- Strings are converted to Date32 if these do not fit into Date range (pre-1970), resulting in Array(Date32)
SELECT generate_date_array('1960-01-01', '1960-01-08') AS example;
SELECT toTypeName(generate_date_array('1960-01-01', '1960-01-08')) AS example;
SELECT toTypeName(generate_date_array('2016-10-05', '2016-10-08')) AS type;
-- String with explicit interval
SELECT generate_date_array('2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS example;
-- Mixed: string and Date32 (result should be Array(Date32) as the supertype)
SELECT generate_date_array('2016-10-05', '2016-10-08'::Date32) AS example;
SELECT toTypeName(generate_date_array('2016-10-05', '2016-10-08'::Date32)) AS type;

-- Date32 and Mixed input types
-- Date32 with default step
SELECT generate_date_array('2016-10-05'::Date32, '2016-10-08'::Date32) AS example;
-- Date32 with explicit interval
SELECT generate_date_array('2016-10-05'::Date32, '2016-10-09'::Date32, INTERVAL 2 DAY) AS example;
-- Date32 with MONTH interval
SELECT generate_date_array('2016-01-01'::Date32, '2016-06-30'::Date32, INTERVAL 1 MONTH) AS example;
-- Mixed: Date and Date32 (result should be Date32 as the supertype)
SELECT generate_date_array('2016-10-05'::Date, '2016-10-08'::Date32) AS example;
SELECT toTypeName(generate_date_array('2016-10-05'::Date, '2016-10-08'::Date32)) AS type;

-- Ranges that end exactly at the boundary of the date type
SELECT generate_date_array('2149-06-03'::Date, '2149-06-06'::Date) AS example;
SELECT generate_date_array('1970-01-04'::Date, '1970-01-01'::Date, INTERVAL -1 DAY) AS example;
SELECT generate_date_array('2299-12-28'::Date32, '2299-12-31'::Date32) AS example;
SELECT generate_date_array('1900-01-04'::Date32, '1900-01-01'::Date32, INTERVAL -1 DAY) AS example;

-- Steps larger than the whole date range produce a single element
SELECT generate_date_array('1970-01-01'::Date, '2149-06-06'::Date, INTERVAL 65537 DAY) AS example;
SELECT generate_date_array('2149-06-06'::Date, '1970-01-01'::Date, INTERVAL -65537 DAY) AS example;
SELECT generate_date_array('1970-01-01'::Date, '2149-06-06'::Date, INTERVAL 65537 YEAR) AS example;
SELECT generate_date_array('1900-01-01'::Date32, '2299-12-31'::Date32, INTERVAL 100000000000 DAY) AS example;

-- Error states
-- ARGUMENT_OUT_OF_BOUND errors
-- Range too large (over 1 million entries). Need to bypass constant folding to trigger it
SELECT generate_date_array(materialize('1900-01-01'), materialize('2299-12-31'), INTERVAL 1 DAY) FROM numbers(10); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- ILLEGAL_COLUMN errors
-- Non-constant step (column reference) is not allowed
SELECT generate_date_array('2024-01-01'::Date, '2024-01-07'::Date, toIntervalDay(number + 1)) FROM numbers(1); -- { serverError ILLEGAL_COLUMN }
-- ILLEGAL_TYPE_OF_ARGUMENT errors
-- Non-date argument types (DateTime, integers) are not supported
SELECT generate_date_array(toDateTime('2016-10-05 00:00:00'), toDateTime('2016-10-08 00:00:00')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generate_date_array(1, 5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Invalid interval kind (sub-day intervals are not allowed)
SELECT generate_date_array('2016-10-05'::Date, '2016-10-06'::Date, INTERVAL 1 HOUR); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generate_date_array('2016-10-05'::Date, '2016-10-06'::Date, INTERVAL 1 MINUTE); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Non-interval third argument
SELECT generate_date_array('2016-10-05'::Date, '2016-10-06'::Date, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- NUMBER_OF_ARGUMENTS_DOESNT_MATCH errors
-- Wrong number of arguments
SELECT generate_date_array('2016-10-05'::Date); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generate_date_array('2016-10-05'::Date, '2016-10-06'::Date, INTERVAL 1 DAY, '2016-10-07'::Date); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- BAD_ARGUMENTS errors
-- Zero step
SELECT generate_date_array('2016-10-05'::Date, '2016-10-06'::Date, INTERVAL 0 DAY); -- { serverError BAD_ARGUMENTS }
-- Nullable step with NULL value
SELECT generate_date_array('2016-10-05'::Date, '2016-10-06'::Date, NULL::Nullable(IntervalDay)) AS example; -- { serverError BAD_ARGUMENTS }
-- Cannot parse string arguments as dates
SELECT generate_date_array('10000-01-01', '10000-01-01') AS example; -- { serverError CANNOT_PARSE_DATE }
