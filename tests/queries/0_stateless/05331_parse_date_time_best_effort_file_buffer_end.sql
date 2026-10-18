-- A best-effort DateTime ending in an alphabetical month at the end of a file, read through a small buffer.
-- With an 8-byte buffer the second read holds only '\n1-Jan', and the byte after it is a stale ' ' from the first read.

INSERT INTO FUNCTION file(currentDatabase() || '_best_effort_month_at_eof.tsv', 'RawBLOB')
SELECT '2-Feb  1\n1-Jan' SETTINGS engine_file_truncate_on_insert = 1;

SELECT formatDateTime(d, '%m-%d %H:%i:%S') AS v
FROM file(currentDatabase() || '_best_effort_month_at_eof.tsv', 'TSV', 'd DateTime')
ORDER BY v
SETTINGS date_time_input_format = 'best_effort', session_timezone = 'UTC', max_read_buffer_size = 8,
    storage_file_read_method = 'pread', input_format_parallel_parsing = 0, input_format_tsv_detect_header = 0;

SELECT formatDateTime(d, '%m-%d %H:%i:%S') AS v
FROM file(currentDatabase() || '_best_effort_month_at_eof.tsv', 'TSV', 'd DateTime')
ORDER BY v
SETTINGS date_time_input_format = 'best_effort', session_timezone = 'UTC',
    storage_file_read_method = 'pread', input_format_parallel_parsing = 0, input_format_tsv_detect_header = 0;
