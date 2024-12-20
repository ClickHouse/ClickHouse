-- Tags: no-fasttest

select formatRow('ORC', number, toDate(number)) from numbers(5); -- { serverError 36 }
select formatRow('Parquet', number, toDate(number)) from numbers(5); -- { serverError 36 }
select formatRow('Arrow', number, toDate(number)) from numbers(5); -- { serverError 36 }
select formatRow('Native', number, toDate(number)) from numbers(5); -- { serverError 36 }
