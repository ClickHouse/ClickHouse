-- Tags: no-backward-compatibility-check

select arrayMap(x -> toNullable(1), range(number)) from numbers(3);
select arrayFilter(x -> toNullable(1), range(number)) from numbers(3);
select arrayMap(x -> toNullable(0), range(number)) from numbers(3);
select arrayFilter(x -> toNullable(0), range(number)) from numbers(3);
select arrayMap(x -> NULL::Nullable(UInt8), range(number)) from numbers(3);
select arrayFilter(x -> NULL::Nullable(UInt8), range(number)) from numbers(3);

