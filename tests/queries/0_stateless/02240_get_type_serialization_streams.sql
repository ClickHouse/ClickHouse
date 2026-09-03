select getTypeSerializationStreams('Array(Int8)');
select getTypeSerializationStreams('Map(String, Int64)');
select getTypeSerializationStreams('Tuple(String, Int64, Float64)');
select getTypeSerializationStreams('LowCardinality(String)');
select getTypeSerializationStreams('Nullable(String)');
select getTypeSerializationStreams([1,2,3]);
select getTypeSerializationStreams(map('a', 1, 'b', 2));
select getTypeSerializationStreams(tuple('a', 1, 'b', 2));
