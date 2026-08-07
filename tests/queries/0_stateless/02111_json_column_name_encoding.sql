-- Tags: no-fasttest

SET output_format_write_statistics = 0;

SELECT
    length('\x80')
    FORMAT JSONCompact;
