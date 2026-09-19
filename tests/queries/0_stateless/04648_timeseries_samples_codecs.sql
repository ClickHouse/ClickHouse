-- Auto-created `timestamp` and `value` columns of the `TimeSeries` samples inner table get compression codecs
-- (`Delta` + `T64` + `ZSTD(3)` for timestamps, `ALP` + `ZSTD(3)` for values). The generation of the codecs is covered by the unit test
-- `gtest_normalize_time_series_definition.cpp`; this test checks that the created inner table has them.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_codecs;
CREATE TABLE ts_codecs ENGINE = TimeSeries;

SELECT name, type, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

DROP TABLE ts_codecs;
