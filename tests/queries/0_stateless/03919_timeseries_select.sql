-- Tags: no-fasttest
-- Test that SELECT works on TimeSeries engine tables.
-- Previously, SELECT on a TimeSeries table threw NOT_IMPLEMENTED.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_select_test;

CREATE TABLE ts_select_test ENGINE = TimeSeries;

-- Verify the table was created
SELECT 'table created';

-- SELECT from empty table should work (returns 0 rows)
SELECT count() FROM ts_select_test;

-- Verify we can select specific data columns
SELECT id, timestamp, value FROM ts_select_test LIMIT 0;

SELECT 'OK: SELECT on TimeSeries table works';

DROP TABLE ts_select_test;
