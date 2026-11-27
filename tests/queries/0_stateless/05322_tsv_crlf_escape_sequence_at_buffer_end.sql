-- An escape sequence that ends exactly at the end of a read buffer with `input_format_tsv_crlf_end_of_line = 1`.
-- With a 16-byte buffer the second read holds only `\t`, and the byte after it is a stale '\r' from the first read.

INSERT INTO FUNCTION file(currentDatabase() || '_tsv_crlf_escape_at_buffer_end.tsv', 'RawBLOB')
SELECT 'ab\rxcdefghijklmn\\t' SETTINGS engine_file_truncate_on_insert = 1;

SELECT * FROM file(currentDatabase() || '_tsv_crlf_escape_at_buffer_end.tsv', 'TSVWithNames', 's String')
SETTINGS input_format_tsv_crlf_end_of_line = 1, max_read_buffer_size = 16, storage_file_read_method = 'pread',
    input_format_parallel_parsing = 0, input_format_with_names_use_header = 0;

SELECT length(s), hex(s) FROM file(currentDatabase() || '_tsv_crlf_escape_at_buffer_end.tsv', 'TSV', 's String')
SETTINGS input_format_tsv_crlf_end_of_line = 1, max_read_buffer_size = 16, storage_file_read_method = 'pread',
    input_format_parallel_parsing = 0, input_format_tsv_detect_header = 0;

SELECT toTypeName(c1), length(c1) FROM file(currentDatabase() || '_tsv_crlf_escape_at_buffer_end.tsv', 'TSV')
SETTINGS input_format_tsv_crlf_end_of_line = 1, max_read_buffer_size = 16, storage_file_read_method = 'pread',
    input_format_parallel_parsing = 0, input_format_tsv_detect_header = 0, schema_inference_make_columns_nullable = 0,
    schema_inference_use_cache_for_file = 0;
