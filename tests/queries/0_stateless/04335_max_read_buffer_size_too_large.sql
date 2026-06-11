-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279
-- Setting max_read_buffer_size to a very large value used to throw std::length_error while
-- allocating a read buffer of that size. The configured value is only an upper bound; the
-- actually allocated buffer must be capped, and reads must keep working.

INSERT INTO FUNCTION file('04335_max_read_buffer_size_too_large.tsv', 'TSV')
    SELECT number FROM numbers(100)
    SETTINGS engine_file_truncate_on_insert = 1;

SET max_read_buffer_size = 18446744073709551615;
SELECT count() FROM file('04335_max_read_buffer_size_too_large.tsv', 'TSV', 'x UInt64');
