-- Tags: no-fasttest
-- Tag justification:
--   no-fasttest: the `S3` engine is not available in fast test.
--
-- The plain object storage engines build no settings object at all: the creator applies the `SETTINGS` clause
-- to a copy of the server settings and converts the result to `FormatSettings`, which carries no setting names
-- to report. They used to report nothing; they now report what their definition states, as `File` and `URL`
-- do. What the definition leaves out is still not reported - there is nothing to read it from.
--
-- Nothing is connected here: the columns are given explicitly, so no schema inference runs.

DROP TABLE IF EXISTS t_s3;

CREATE TABLE t_s3 (a UInt64)
ENGINE = S3('http://localhost:11111/test/table_settings.csv', NOSIGN, 'CSV')
SETTINGS format_csv_delimiter = ';';

SELECT '-- what the definition states is reported';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_s3'
ORDER BY name;

SELECT '-- and the row carries the metadata the engine knows, not an empty type and an assumed tier';
-- Unlike `File` and `URL`, this engine has a settings struct; it just never builds one per table. So the type,
-- default, description and tier of a stated setting are known, and must be the ones the engine advertises -
-- otherwise an obsolete setting reports itself as a `Production` one.
SELECT ts.name, ts.type = es.type, ts.`default` = es.`default`, ts.description = es.description, ts.tier = es.tier
FROM system.table_settings AS ts
INNER JOIN (SELECT * FROM system.engine_settings WHERE engine_name = 'S3') AS es ON es.name = ts.name
WHERE ts.database = currentDatabase() AND ts.table = 't_s3';

DROP TABLE t_s3;

SELECT '-- the metadata is reported for an obsolete setting too, which must not claim to be a production one';
CREATE TABLE t_s3_obsolete (a UInt64)
ENGINE = S3('http://localhost:11111/test/table_settings.parquet', NOSIGN, 'Parquet')
SETTINGS input_format_parquet_import_nested = 1;
SELECT name, tier, is_obsolete FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_s3_obsolete';
DROP TABLE t_s3_obsolete;
