-- IN uses strict (exact) Field conversion for its set elements. A set element whose fractional
-- part does not survive rescaling to the lower scale of the left-hand side must be excluded
-- from the set instead of being truncated into a spurious match.
SET enable_time_time64_type = 1;
SET session_timezone = 'UTC';

SELECT 'Time64 -> Time64, scale-reducing, inexact';
SELECT toTime64('00:00:00', 0) IN (CAST('00:00:00.5', 'Time64(1)'));
SELECT toTime64('00:00:00', 0) IN (CAST('00:00:00.9', 'Time64(1)'));
SELECT toTime64('00:00:01', 0) IN (CAST('00:00:01.001', 'Time64(3)'));
SELECT toTime64('00:00:00.10', 2) IN (CAST('00:00:00.123', 'Time64(3)'));
SELECT toTime64('-00:00:01', 0) IN (CAST('-00:00:01.5', 'Time64(1)'));
SELECT 'Time64 -> Time64, scale-reducing, exact';
SELECT toTime64('00:00:00', 0) IN (CAST('00:00:00.0', 'Time64(1)'));
SELECT toTime64('00:00:01', 0) IN (CAST('00:00:01.000', 'Time64(3)'));
SELECT toTime64('00:00:00.10', 2) IN (CAST('00:00:00.100', 'Time64(3)'));
SELECT toTime64('-00:00:01', 0) IN (CAST('-00:00:01.0', 'Time64(1)'));
SELECT 'Time64 -> Time64, scale-widening and equal scale';
SELECT toTime64('00:00:00.100', 3) IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT toTime64('00:00:00.1', 1) IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT toTime64('00:00:00.2', 1) IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT 'Time64 -> Time64, multiple elements: only the exactly representable one matches';
SELECT toTime64('00:00:02', 0) IN (CAST('00:00:00.5', 'Time64(1)'), CAST('00:00:02.0', 'Time64(1)'));
SELECT toTime64('00:00:00', 0) IN (CAST('00:00:00.5', 'Time64(1)'), CAST('00:00:02.0', 'Time64(1)'));

-- The same rescaling path serves DateTime64 set elements.
SELECT 'DateTime64 -> DateTime64, scale-reducing';
SELECT toDateTime64('1970-01-01 00:00:00', 0) IN (toDateTime64('1970-01-01 00:00:00.5', 1));
SELECT toDateTime64('1970-01-01 00:00:00', 0) IN (toDateTime64('1970-01-01 00:00:00.0', 1));
SELECT toDateTime64('1970-01-01 00:00:00.10', 2) IN (toDateTime64('1970-01-01 00:00:00.123', 3));
SELECT toDateTime64('1970-01-01 00:00:00.10', 2) IN (toDateTime64('1970-01-01 00:00:00.100', 3));

-- Non-strict conversions (CAST, INSERT) keep truncating as before.
SELECT 'non-strict CAST still truncates';
SELECT CAST(CAST('00:00:00.5', 'Time64(1)'), 'Time64(0)');
SELECT CAST(toDateTime64('1970-01-01 00:00:00.5', 1), 'DateTime64(0)');

-- The key condition must keep returning the matching rows and skip inexact set elements.
DROP TABLE IF EXISTS t_time64_strict_in;
CREATE TABLE t_time64_strict_in (t Time64(0)) ENGINE = MergeTree ORDER BY t;
INSERT INTO t_time64_strict_in VALUES ('00:00:00'), ('00:00:01'), ('00:00:02');
SELECT 'MergeTree key: Time64(0) IN inexact Time64(1) set';
SELECT t FROM t_time64_strict_in WHERE t IN (CAST('00:00:00.5', 'Time64(1)'), CAST('00:00:02.0', 'Time64(1)')) ORDER BY t;
DROP TABLE t_time64_strict_in;
