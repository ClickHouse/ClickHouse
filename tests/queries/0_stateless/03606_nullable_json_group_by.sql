select '{"a" : 42}'::Nullable(JSON) group by 1;
select materialize('{"a" : 42}')::Nullable(JSON) group by 1;
select null::Nullable(JSON) group by 1;
select materialize(null)::Nullable(JSON) group by 1;
select (number % 2 ? null : '{"a" : 42}')::Nullable(JSON) as a from numbers(4) group by 1 order by a;

