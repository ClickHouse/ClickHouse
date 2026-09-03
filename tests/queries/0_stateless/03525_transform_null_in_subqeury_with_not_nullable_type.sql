set transform_null_in=1;

select null::Nullable(String) in (select 'abc');
select (null::Nullable(String), 42) in (select 'abc', 42);
select (null::Nullable(String), null::Nullable(UInt32)) in (select 'abc', 42);

select (number % 2 ? null : 'abc') in (select 'abc') from numbers(2);
select (number % 2 ? null : 'abc', materialize(42)) in (select 'abc', 42) from numbers(2);
select (number % 2 == 0 ? null : 'abc', number < 2 ? null : 42) in (select 'abc', 42) from numbers(4);

