-- Test for issue #95913: query parameter values were parsed with default format
-- settings, ignoring the settings of the session such as `date_time_input_format`.

set param_dt = '2026-02-03T21:03:24Z';

select 'best_effort parses an ISO 8601 value with a timezone suffix';
set date_time_input_format = 'best_effort';
select {dt:DateTime('UTC')};

select 'basic rejects the timezone suffix';
set date_time_input_format = 'basic';
select {dt:DateTime('UTC')}; -- { serverError BAD_QUERY_PARAMETER }
