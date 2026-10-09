-- Tags: no-fasttest
-- no-fasttest: Arrow format is not available in fasttest builds

-- Write a single uncompressed record batch of ~30 MiB, so that reading it back
-- requires one allocation larger than the memory limit used below.
INSERT INTO FUNCTION file(currentDatabase() || '_04335.arrow', Arrow)
    SELECT number AS x, repeat('a', 3000) AS s FROM numbers(10000)
    SETTINGS max_block_size = 65536, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1, output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04335.arrows', ArrowStream)
    SELECT number AS x, repeat('a', 3000) AS s FROM numbers(10000)
    SETTINGS max_block_size = 65536, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1, output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04335.orc', ORC)
    SELECT number AS x, repeat('a', 3000) AS s FROM numbers(10000)
    SETTINGS max_block_size = 65536, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1, output_format_orc_compression_method = 'none', engine_file_truncate_on_insert = 1;

-- Reaching the memory limit while parsing Arrow data must fail the query with
-- MEMORY_LIMIT_EXCEEDED, not CANNOT_READ_ALL_DATA.
-- The structure is specified explicitly to avoid schema inference, which wraps
-- all errors into CANNOT_EXTRACT_TABLE_STRUCTURE.
SELECT sum(length(s)) FROM file(currentDatabase() || '_04335.arrow', Arrow, 'x UInt64, s String') SETTINGS max_memory_usage = 20000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(s)) FROM file(currentDatabase() || '_04335.arrows', ArrowStream, 'x UInt64, s String') SETTINGS max_memory_usage = 20000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
-- The legacy Arrow-based ORC reader (the fast decoder is a separate code path).
SELECT sum(length(s)) FROM file(currentDatabase() || '_04335.orc', ORC, 'x UInt64, s String') SETTINGS max_memory_usage = 20000000, input_format_orc_use_fast_decoder = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
