-- Tags: no-fasttest
-- Tag no-fasttest: needs Arrow support built in.

-- `input_format_arrow_use_native_reader` and `output_format_arrow_use_native_writer` were made obsolete
-- together with the removal of the Apache Arrow library-based reader and writer for the `Arrow` and
-- `ArrowStream` formats: the native implementation is now always used. Both settings are still accepted
-- for backward compatibility, but have no effect. This test keeps that compatibility path exercised in
-- CI, mirroring `04513_orc_fast_decoder_obsolete_setting` for ORC.

-- Writing with the obsolete writer setting explicitly disabled must still go through the native writer.
INSERT INTO FUNCTION file(currentDatabase() || '_04648.arrow', 'Arrow')
SELECT number AS x, toString(number) AS s FROM numbers(100)
SETTINGS output_format_arrow_use_native_writer = 0, engine_file_truncate_on_insert = 1;

-- Reading with the obsolete reader setting explicitly disabled must still succeed through the native reader.
SELECT count(), sum(x), sum(length(s)) FROM file(currentDatabase() || '_04648.arrow', 'Arrow')
SETTINGS input_format_arrow_use_native_reader = 0;

-- Both obsolete settings are accepted for the opposite value too, and are ignored.
INSERT INTO FUNCTION file(currentDatabase() || '_04648.arrow', 'Arrow')
SELECT number AS x, toString(number) AS s FROM numbers(100)
SETTINGS output_format_arrow_use_native_writer = 1, engine_file_truncate_on_insert = 1;

SELECT count(), sum(x), sum(length(s)) FROM file(currentDatabase() || '_04648.arrow', 'Arrow')
SETTINGS input_format_arrow_use_native_reader = 1;

-- Schema inference through the native reader is unaffected by the obsolete setting.
DESC file(currentDatabase() || '_04648.arrow', 'Arrow') SETTINGS input_format_arrow_use_native_reader = 0;

-- The same for the streaming format.
INSERT INTO FUNCTION file(currentDatabase() || '_04648.arrows', 'ArrowStream')
SELECT number AS x FROM numbers(10)
SETTINGS output_format_arrow_use_native_writer = 0, engine_file_truncate_on_insert = 1;

SELECT count(), sum(x) FROM file(currentDatabase() || '_04648.arrows', 'ArrowStream')
SETTINGS input_format_arrow_use_native_reader = 0;
