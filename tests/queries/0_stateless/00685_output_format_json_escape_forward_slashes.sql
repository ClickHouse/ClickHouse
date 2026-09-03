-- Tags: no-fasttest

SET output_format_json_escape_forward_slashes = 1;
select '/some/cool/url' as url format JSONEachRow;
SET output_format_json_escape_forward_slashes = 0;
select '/some/cool/url' as url format JSONEachRow;
