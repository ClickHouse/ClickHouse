set session_timezone='UTC';

set cast_string_to_date_time_mode='basic';
select '2020-02-01 20:00:00'::DateTime;
select '2020-02-01 20:00:00Z'::DateTime; -- {serverError CANNOT_PARSE_TEXT}
select '01-02-2020 20:00:00Z'::DateTime; -- {serverError CANNOT_PARSE_TEXT}

set cast_string_to_date_time_mode='best_effort';
select '2020-02-01 20:00:00'::DateTime;
select '2020-02-01 20:00:00Z'::DateTime;
select '01-02-2020 20:00:00Z'::DateTime;

set cast_string_to_date_time_mode='best_effort_us';
select '2020-02-01 20:00:00'::DateTime;
select '2020-02-01 20:00:00Z'::DateTime;
select '01-02-2020 20:00:00Z'::DateTime;

