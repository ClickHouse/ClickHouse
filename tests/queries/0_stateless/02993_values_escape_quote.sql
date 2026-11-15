select 'foo' format Values;
select 'foo\'bar' format Values;
select 'foo\'\'bar' format Values;

select '\noutput_format_values_escape_quote_with_quote=1' format LineAsString;
set output_format_values_escape_quote_with_quote=1;

select 'foo' format Values;
select 'foo\'bar' format Values;
select 'foo\'\'bar' format Values;
-- fix no newline at end of file
select '' format LineAsString;
