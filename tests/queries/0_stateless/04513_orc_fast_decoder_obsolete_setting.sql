-- Tags: no-fasttest
-- Tag no-fasttest: needs ORC support built in.

-- `input_format_orc_use_fast_decoder` was made obsolete together with the removal of the
-- legacy Apache Arrow-based ORC reader: the native ORC reader is now always used. The setting
-- is still accepted for backward compatibility, but has no effect. This test keeps that
-- compatibility path exercised in CI, mirroring the Parquet coverage for the obsolete
-- `input_format_parquet_use_native_reader_v3` setting.

INSERT INTO FUNCTION file(currentDatabase() || '_04513.orc', 'ORC')
SELECT number AS x, toString(number) AS s FROM numbers(100)
SETTINGS engine_file_truncate_on_insert = 1;

-- Reading with the obsolete setting explicitly disabled must still succeed through the native reader.
SELECT count(), sum(x), sum(length(s)) FROM file(currentDatabase() || '_04513.orc', 'ORC')
SETTINGS input_format_orc_use_fast_decoder = 0;

-- The obsolete setting is accepted for the opposite value too, and is ignored.
SELECT count(), sum(x), sum(length(s)) FROM file(currentDatabase() || '_04513.orc', 'ORC')
SETTINGS input_format_orc_use_fast_decoder = 1;

-- Schema inference through the native reader is unaffected by the obsolete setting.
DESC file(currentDatabase() || '_04513.orc', 'ORC') SETTINGS input_format_orc_use_fast_decoder = 0;
