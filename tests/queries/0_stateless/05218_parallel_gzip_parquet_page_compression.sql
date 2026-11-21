-- Tags: no-fasttest
-- Parquet is not available in fasttest builds.

-- `max_generic_compression_threads` must not reach the Parquet page compressor: it hands the gzip writer an
-- external working buffer (`existing_memory`) and advances the returned buffer over an already filled page,
-- a contract the parallel deflater does not honour. The wrapper keeps such callers on the serial writers,
-- so gzip Parquet output must stay a valid round-trip with the setting enabled.

SET max_generic_compression_threads = 8;
SET output_format_parquet_compression_method = 'gzip';
SET engine_file_truncate_on_insert = 1;

-- Default level: libdeflate when available, zlib otherwise.
INSERT INTO FUNCTION file(currentDatabase() || '_05218_gzip.parquet', 'Parquet', 'n UInt64, s String')
SELECT number, repeat(toString(number), 10) FROM numbers(200000);

SELECT count(), sum(n), sum(length(s)) FROM file(currentDatabase() || '_05218_gzip.parquet', 'Parquet', 'n UInt64, s String');
SELECT n, s FROM file(currentDatabase() || '_05218_gzip.parquet', 'Parquet', 'n UInt64, s String') WHERE n IN (0, 12345, 199999) ORDER BY n;

-- Level 0 is always deflated by zlib, so the zlib serial writer is exercised even with libdeflate.
SET output_format_compression_level = 0;
INSERT INTO FUNCTION file(currentDatabase() || '_05218_gzip_l0.parquet', 'Parquet', 'n UInt64, s String')
SELECT number, repeat(toString(number), 10) FROM numbers(200000);

SELECT count(), sum(n), sum(length(s)) FROM file(currentDatabase() || '_05218_gzip_l0.parquet', 'Parquet', 'n UInt64, s String');
