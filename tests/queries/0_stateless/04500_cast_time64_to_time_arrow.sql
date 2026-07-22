-- Tags: no-fasttest
-- no-fasttest: Arrow format is not available in the fast test build.

-- Arrow stores Time as Time64(0), so reading a Time column back needs the Time64 -> Time cast.
SET enable_time_time64_type = 1;
INSERT INTO FUNCTION file('04500_time64_to_time.arrow', Arrow, 'c1 Time') VALUES ('10:00:00');
SELECT c1 = CAST('10:00:00' AS Time) FROM file('04500_time64_to_time.arrow', Arrow, 'c1 Time');
