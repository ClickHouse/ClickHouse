-- Tags: no-fasttest
-- no-fasttest: Arrow format is not available in the fast test build.

-- Arrow stores Time as Time64(0), so reading a Time column back needs the Time64 -> Time cast.
SET enable_time_time64_type = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04500_time64_to_time.arrow', Arrow, 'c1 Time') SETTINGS engine_file_truncate_on_insert = 1 VALUES ('10:00:00');
SELECT c1 = CAST('10:00:00' AS Time) FROM file(currentDatabase() || '_04500_time64_to_time.arrow', Arrow, 'c1 Time');

-- The original report used a LowCardinality(Nullable(Time)) schema.
SET allow_suspicious_low_cardinality_types = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04500_time64_to_time_lc.arrow', Arrow, 'c1 LowCardinality(Nullable(Time))') SETTINGS engine_file_truncate_on_insert = 1 VALUES ('10:00:00'), (NULL);
SELECT c1 = CAST('10:00:00' AS Time), isNull(c1) FROM file(currentDatabase() || '_04500_time64_to_time_lc.arrow', Arrow, 'c1 LowCardinality(Nullable(Time))') ORDER BY c1 ASC NULLS LAST;
