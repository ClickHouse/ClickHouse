-- Test for issue #95913: query parameter values were parsed with default format
-- settings, ignoring the settings of the session such as `date_time_input_format`.

set param_dt = '2026-02-03T21:03:24Z';

select 'best_effort parses an ISO 8601 value with a timezone suffix';
set date_time_input_format = 'best_effort';
select {dt:DateTime('UTC')};

select 'basic rejects the timezone suffix';
set date_time_input_format = 'basic';
select {dt:DateTime('UTC')}; -- { serverError BAD_QUERY_PARAMETER }

select 'parameters in INSERT VALUES respect the setting';
set date_time_input_format = 'best_effort';
drop table if exists t_04614;
create table t_04614 (d DateTime('UTC')) engine = Memory;
insert into t_04614 values ({dt:DateTime('UTC')});
select * from t_04614;
drop table t_04614;
