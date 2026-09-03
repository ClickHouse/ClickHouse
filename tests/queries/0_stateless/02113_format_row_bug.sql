-- Tags: no-fasttest

select formatRow('ORC', number, toDate(number)) from numbers(5); -- { serverError BAD_ARGUMENTS }
select formatRow('Parquet', number, toDate(number)) from numbers(5); -- { serverError BAD_ARGUMENTS }
select formatRow('Arrow', number, toDate(number)) from numbers(5); -- { serverError BAD_ARGUMENTS }
select formatRow('Native', number, toDate(number)) from numbers(5); -- { serverError BAD_ARGUMENTS }
