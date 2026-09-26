-- Auto-created `timestamp` and `value` columns of a version 5 `TimeSeries` samples inner table get compression codecs
-- (`Delta` + `T64` + `ZSTD(3)` for timestamps, `ALP` + `ZSTD(3)` for values). The auto-created `samples` column of
-- a version 7 table gets `ZSTD(3)`. Codec generation is covered by the unit test
-- `gtest_normalize_time_series_definition.cpp`; this test checks that the created inner tables have the codecs.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_codecs;

SELECT 'version 5:';
CREATE TABLE ts_codecs ENGINE = TimeSeries SETTINGS version = 5;
SELECT name, type, if(empty(compression_codec), '<none>', compression_codec) AS compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;
DROP TABLE ts_codecs;

SELECT 'version 6:';
CREATE TABLE ts_codecs ENGINE = TimeSeries SETTINGS version = 6;
SELECT name, type, if(empty(compression_codec), '<none>', compression_codec) AS compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;
DROP TABLE ts_codecs;

SELECT 'version 7:';
CREATE TABLE ts_codecs ENGINE = TimeSeries SETTINGS version = 7;
SELECT name, type, if(empty(compression_codec), '<none>', compression_codec) AS compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;
DROP TABLE ts_codecs;
