-- Test that FixedString respects the output_format_values_escape_quote_with_quote setting
-- This was broken: https://github.com/ClickHouse/ClickHouse/issues/73519

-- Default behavior (backslash escaping)
select toFixedString('\'', 4) format Values;
select toFixedString('foo\'bar', 8) format Values;

select '\noutput_format_values_escape_quote_with_quote=1' format LineAsString;
set output_format_values_escape_quote_with_quote=1;

-- PostgreSQL/SQLite style (quote doubling)
select toFixedString('\'', 4) format Values;
select toFixedString('foo\'bar', 8) format Values;

-- Ensure no newline issues at end of file
select '' format LineAsString;
