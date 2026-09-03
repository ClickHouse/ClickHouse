select * from format('RowBinaryWithDefaults', 'x UInt32 default 42', x'01');
select * from format('RowBinaryWithDefaults', 'x UInt32 default 42', x'0001000000');
select * from format('RowBinaryWithDefaults', 'x Nullable(UInt32) default 42', x'01');
select * from format('RowBinaryWithDefaults', 'x Nullable(UInt32) default 42', x'000001000000');
select * from format('RowBinaryWithDefaults', 'x Nullable(UInt32) default 42', x'0001');
select * from format('RowBinaryWithDefaults', 'x Array(Tuple(UInt32, UInt32)) default [(42, 42)]', x'01');

