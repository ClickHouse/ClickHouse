set function_date_trunc_use_datetime64_and_date32_return_type_on_datetime64_and_date32_arguments=0;
set session_timezone='UTC';
select dateTrunc('second', '2020-10-10 10:10:10.10'::DateTime64(2)) as result, toTypeName(result);
select dateTrunc('month', '2020-10-10'::Date32) as result, toTypeName(result);
