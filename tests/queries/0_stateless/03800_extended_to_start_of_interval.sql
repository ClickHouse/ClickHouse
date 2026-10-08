SET enable_extended_results_for_datetime_functions = 1;
-- We want toStartOfInterval to give exactly the same results as toStartOfX, where X is any type of interval

-- Nanoseconds
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 8, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 nanosecond) as a,
        toStartOfNanosecond(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 8, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 nanosecond) as a,
        toStartOfNanosecond(dt) as b;

WITH toDateTime64(-79999999.9761238, 8, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 nanosecond) as a,
        toStartOfNanosecond(dt) as b;

-- Microseconds
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 6, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 microsecond) as a,
        toStartOfMicrosecond(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 6, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 microsecond) as a,
        toStartOfMicrosecond(dt) as b;

WITH toDateTime64(-79999999.9761238, 6, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 microsecond) as a,
        toStartOfMicrosecond(dt) as b;

-- Milliseconds
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 millisecond) as a,
        toStartOfMillisecond(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 millisecond) as a,
        toStartOfMillisecond(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 millisecond) as a,
        toStartOfMillisecond(dt) as b;


-- Seconds
-- DateTime64
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 second) as a,
        toStartOfSecond(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 second) as a,
        toStartOfSecond(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 second) as a,
        toStartOfSecond(dt) as b;

-- DateTime32
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 second) as a,
        toStartOfSecond(dt) as b;

-- Minutes
-- DateTime64
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 minute) as a,
        toStartOfMinute(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 minute) as a,
        toStartOfMinute(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 minute) as a,
        toStartOfMinute(dt) as b;

-- DateTime32
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 minute) as a,
        toStartOfMinute(dt) as b;

-- Hours
-- DateTime64
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 hour) as a,
        toStartOfHour(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 hour) as a,
        toStartOfHour(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 hour) as a,
        toStartOfHour(dt) as b;

-- DateTime32
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 hour) as a,
        toStartOfHour(dt) as b;

-- Days
-- DateTime64
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;
-- Date32
-- Extended In Bounds
WITH toDate32(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;

-- DateTime
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;
-- Date
WITH toDate(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 day) as a,
        toStartOfDay(dt) as b;

-- Weeks
-- Day32, DayTime64
-- Day, DayTime
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;
-- Date32
-- Extended In Bounds
WITH toDate32(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;

-- DateTime
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;
-- Date
WITH toDate(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 week) as a,
        toStartOfWeek(dt) as b;

-- Months
-- Day, DayTime
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;
-- Date32
-- Extended In Bounds
WITH toDate32(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;

-- DateTime
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;
-- Date
WITH toDate(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 month) as a,
        toStartOfMonth(dt) as b;


-- Quarters
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;
-- Date32
-- Extended In Bounds
WITH toDate32(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;

-- DateTime
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;
-- Date
WITH toDate(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 quarter) as a,
        toStartOfQuarter(dt) as b;


-- Years
-- Extended In Bounds
WITH toDateTime64(23119945.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(799999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;
-- Date32
-- Extended In Bounds
WITH toDate32(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;
-- Extended Out of Bounds
WITH toDateTime64(7999999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;

WITH toDateTime64(-79999999.9761238, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;

-- DateTime
WITH toDateTime(23119945, 3, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;
-- Date
WITH toDate(23119945, 'UTC' ) as dt
SELECT
        toStartOfInterval(dt, INTERVAL 1 year) as a,
        toStartOfYear(dt) as b;
