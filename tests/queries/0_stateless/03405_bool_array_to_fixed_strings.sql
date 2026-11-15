select [true, false]::Array(FixedString(5));

select arrayMap(x -> x::FixedString(5), [true, false]);

select [false, true]::Array(FixedString(5));
