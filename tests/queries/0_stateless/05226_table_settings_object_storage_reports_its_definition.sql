-- Tags: no-fasttest
-- Tag justification:
--   no-fasttest: the `S3` engine is not available in fast test.
--
-- The plain object storage engines build no settings object at all: the creator applies the `SETTINGS` clause
-- to a copy of the server settings and converts the result to `FormatSettings`, which carries no setting names
-- to report. They used to report nothing; they now report what their definition states, as `File`, `URL` and
-- the `Log` family do. What the definition leaves out is still not reported - there is nothing to read it from.
--
-- Nothing is connected here: the columns are given explicitly, so no schema inference runs.

DROP TABLE IF EXISTS t_s3;

CREATE TABLE t_s3 (a UInt64)
ENGINE = S3('http://localhost:11111/test/table_settings.csv', NOSIGN, 'CSV')
SETTINGS input_format_csv_delimiter = ';';

SELECT '-- what the definition states is reported';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_s3'
ORDER BY name;

DROP TABLE t_s3;
