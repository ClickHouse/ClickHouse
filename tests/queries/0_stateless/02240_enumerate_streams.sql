select enumerateStreams('Array(Int8)');
select enumerateStreams('Map(String, Int64)');
select enumerateStreams('Tuple(String, Int64, Float64)');
select enumerateStreams('LowCardinality(String)');
select enumerateStreams('Nullable(String)');
select enumerateStreams([1,2,3]);
select enumerateStreams(map('a', 1, 'b', 2));
select enumerateStreams(tuple('a', 1, 'b', 2));
