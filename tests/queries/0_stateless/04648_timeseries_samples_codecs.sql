-- The auto-created `samples` column of the `TimeSeries` samples inner table gets time-series compression codecs declared
-- for the elements of its tuples: `Delta, T64, ZSTD(3)` for the timestamps and `ALP, ZSTD(3)` for the values.
-- The auto-created `bucket`, `min_time` and `max_time` columns get the codec of the timestamps.
-- The generation of the codecs is covered by the unit test `gtest_normalize_time_series_definition.cpp`;
-- this test checks that the created inner table has them.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_codecs;
CREATE TABLE ts_codecs ENGINE = TimeSeries;

SELECT name, type, compression_codec, subcolumn_codecs FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

DROP TABLE ts_codecs;
