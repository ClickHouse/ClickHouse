SELECT accurateCast(-1, 'UInt8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt8');
SELECT accurateCast(257, 'UInt8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-1, 'UInt16'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt16');
SELECT accurateCast(65536, 'UInt16'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-1, 'UInt32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt32');
SELECT accurateCast(4294967296, 'UInt32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-1, 'UInt64'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt64');
SELECT accurateCast(-1, 'UInt256'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt256');

SELECT accurateCast(-129, 'Int8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'Int8');
SELECT accurateCast(128, 'Int8'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT accurateCast('-1', 'UInt8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt8');
SELECT accurateCast('257', 'UInt8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('-1', 'UInt16'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt16');
SELECT accurateCast('65536', 'UInt16'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('-1', 'UInt32'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt32');
SELECT accurateCast('4294967296', 'UInt32'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('-1', 'UInt64'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt64');
SELECT accurateCast('-129', 'Int8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'Int8');
SELECT accurateCast('128', 'Int8'); -- { serverError CANNOT_PARSE_TEXT }

SELECT accurateCast(10, 'Decimal32(9)'); -- { serverError DECIMAL_OVERFLOW }
SELECT accurateCast(1, 'Decimal32(9)');
SELECT accurateCast(-10, 'Decimal32(9)'); -- { serverError DECIMAL_OVERFLOW }

SELECT accurateCast('123', 'FixedString(2)'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT accurateCast('123', 'Nullable(FixedString(2))'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT accurateCast('12', 'FixedString(2)');

SELECT accurateCast(-1, 'DateTime');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(0xFFFFFFFF + 1, 'DateTime');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast('1xxx', 'DateTime');   -- { serverError CANNOT_PARSE_DATETIME }
SELECT accurateCast('1xxx', 'Nullable(DateTime)');   -- { serverError CANNOT_PARSE_DATETIME }
SELECT accurateCast('2023-05-30 14:38:20', 'DateTime');
SELECT toString(accurateCast(19, 'DateTime'), 'UTC');

SELECT accurateCast(-1, 'Date');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(0xFFFFFFFF + 1, 'Date');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast('1xxx', 'Date');   -- { serverError CANNOT_PARSE_DATE }
SELECT accurateCast('1xxx', 'Nullable(Date)');   -- { serverError CANNOT_PARSE_DATE }
SELECT accurateCast('2023-05-30', 'Date');
SELECT accurateCast(19, 'Date');

select accurateCast('test', 'Nullable(Bool)');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('test', 'Bool');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('truex', 'Bool');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('xfalse', 'Bool');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('true', 'Bool');
select accurateCast('false', 'Bool');
select accurateCast('1', 'Bool');
select accurateCast('0', 'Bool');
select accurateCast(1, 'Bool');
select accurateCast(0, 'Bool');

select accurateCast('test', 'Nullable(IPv4)');   -- { serverError CANNOT_PARSE_IPV4 }
select accurateCast('test', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }
select accurateCast('2001:db8::1', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }
select accurateCast('::ffff:192.0.2.1', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }
select accurateCast('192.0.2.1', 'IPv4');
select accurateCast('192.0.2.1x', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }

select accurateCast('test', 'Nullable(IPv6)');   -- { serverError CANNOT_PARSE_IPV6 }
select accurateCast('test', 'IPv6');   -- { serverError CANNOT_PARSE_IPV6 }
select accurateCast('192.0.2.1', 'IPv6');
select accurateCast('2001:db8::1', 'IPv6');
select accurateCast('2001:db8::1x', 'IPv6');   -- { serverError CANNOT_PARSE_IPV6 }

-- A Nullable target does not soften accurateCast: the value the target cannot represent raises the
-- same error as the non-Nullable arm printed beside it.
select accurateCast('abc', 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('abc', 'Int32');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('999999999999', 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('999999999999', 'Int32');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('abc', 'Nullable(Float32)');   -- { serverError CANNOT_PARSE_NUMBER }
select accurateCast('abc', 'Float32');   -- { serverError CANNOT_PARSE_NUMBER }
select accurateCast('abc', 'Nullable(Decimal(10, 2))');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('abc', 'Decimal(10, 2)');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('abc', 'Nullable(UUID)');   -- { serverError CANNOT_PARSE_UUID }
select accurateCast('abc', 'UUID');   -- { serverError CANNOT_PARSE_UUID }
select accurateCast(materialize('abc'), 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }

-- Text that parses but does not fit, or does not parse whole.
select accurateCast('42.9', 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('  42  ', 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('0x1A', 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast('1e3', 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }

-- Values the target can represent come out exactly as they do without the Nullable.
select accurateCast('42', 'Nullable(Int32)'), accurateCast('42', 'Int32');
select accurateCast('+42', 'Nullable(Int32)'), accurateCast('+42', 'Int32');
select accurateCast('-0', 'Nullable(Int32)'), accurateCast('-0', 'Int32');
select accurateCast('1e39', 'Nullable(Float32)'), accurateCast('1e39', 'Float32');
select accurateCast('nan', 'Nullable(Float64)'), accurateCast('nan', 'Float64');
select accurateCast('inf', 'Nullable(Float64)'), accurateCast('inf', 'Float64');
select accurateCast('2020-1-1', 'Nullable(Date)'), accurateCast('2020-1-1', 'Date');
select accurateCast('20200101', 'Nullable(Date)'), accurateCast('20200101', 'Date');
select accurateCast('0.005', 'Nullable(Decimal(9, 2))'), accurateCast('0.005', 'Decimal(9, 2)');
select hex(accurateCast('a', 'Nullable(FixedString(2))')), hex(accurateCast('a', 'FixedString(2)'));
select accurateCast('1.2.3.4', 'Nullable(IPv4)'), accurateCast('1.2.3.4', 'IPv4');
select accurateCast('2001:db8::1', 'Nullable(IPv6)'), accurateCast('2001:db8::1', 'IPv6');

-- A NULL in the result comes from the source, never from a value the conversion could not represent,
-- for each shape of the source null map: none, all, mixed.
select accurateCast(CAST(NULL, 'Nullable(String)'), 'Nullable(Int32)');
select accurateCast(x, 'Nullable(Int32)') from values('x Nullable(String)', ('7'), ('8'));
select accurateCast(x, 'Nullable(Int32)') from values('x Nullable(String)', ('7'), ('zz'));   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast(x, 'Nullable(Int32)') from values('x Nullable(String)', (NULL), (NULL), (NULL));
select accurateCast(x, 'Nullable(Int32)') from values('x Nullable(String)', ('7'), (NULL));
select accurateCast(x, 'Nullable(Int32)') from values('x Nullable(String)', ('7'), (NULL), ('zzz'));   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast(x, 'Nullable(Int32)') from values('x Nullable(String)', ('7')) where 0;
-- An identity conversion is not routed through the NULL-row filter, and still returns each source NULL.
select accurateCast(x, 'Nullable(String)') from values('x Nullable(String)', ('a'), (NULL), ('b'));

-- Nested elements are converted per level, so a Nullable element behaves like a top-level target.
select accurateCast(materialize(['1', '2']), 'Array(Nullable(Int32))');
select accurateCast(materialize(['1', 'zz']), 'Array(Nullable(Int32))');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast(materialize([CAST('5', 'Nullable(String)'), NULL]), 'Array(Nullable(Int32))');
select accurateCast(m, 'Map(String, Nullable(Int32))') from values('m Map(String, String)', (map('k', 'zz')));   -- { serverError CANNOT_PARSE_TEXT }

-- A row masked by an enclosing Nullable level is not converted at all, at any depth.
select accurateCast(t, 'Nullable(Tuple(Nullable(Int32)))') from values('t Nullable(Tuple(String))', (NULL), (tuple('7')));
select accurateCast(t, 'Nullable(Tuple(Nullable(Int32)))') from values('t Nullable(Tuple(String))', (NULL), (tuple('zz')));   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast(t, 'Nullable(Tuple(Nullable(Int32)))') from values('t Nullable(Tuple(Nullable(String)))', (tuple(NULL)), (tuple('7')));
select accurateCast(t, 'Nullable(Tuple(Nullable(Tuple(Nullable(Int32)))))') from values('t Nullable(Tuple(Nullable(Tuple(String))))', (nullIf(tuple(tuple('zz')), tuple(tuple('zz')))));
select accurateCast(t, 'Nullable(Tuple(Nullable(String), Nullable(Int32)))') from values('t Nullable(Tuple(LowCardinality(String), String))', (NULL), (tuple('a', '7')), (NULL), (tuple('b', '8')));
select accurateCast(t, 'Nullable(Tuple(Nullable(String), Nullable(Int32)))') from values('t Nullable(Tuple(LowCardinality(String), String))', (NULL), (tuple('a', 'zz')));   -- { serverError CANNOT_PARSE_TEXT }

-- LowCardinality is unwrapped before the conversion is chosen, so it is in scope either way round.
select accurateCast(materialize(toLowCardinality('7')), 'Nullable(Int32)');
select accurateCast(materialize(toLowCardinality('zzz')), 'Nullable(Int32)');   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast(x, 'Nullable(Int32)') from values('x LowCardinality(Nullable(String))', ('7'), (NULL));

-- A FixedString source reaches the same NULL-on-error branches as a String one.
select accurateCast(CAST('abcd', 'FixedString(4)'), 'Nullable(FixedString(2))');   -- { serverError TOO_LARGE_STRING_SIZE }
select accurateCast(CAST('test', 'FixedString(4)'), 'Nullable(IPv6)');   -- { serverError CANNOT_PARSE_IPV6 }
select accurateCast(CAST('test', 'FixedString(4)'), 'Nullable(IPv4)');   -- { serverError ILLEGAL_COLUMN }

-- A String variant of Variant/Dynamic is converted by the same per-variant recursion.
select accurateCast(CAST('abc', 'Variant(String, Int64)'), 'Nullable(Int32)') settings enable_variant_type = 1;   -- { serverError CANNOT_PARSE_TEXT }
select accurateCast(CAST('42', 'Variant(String, Int64)'), 'Nullable(Int32)') settings enable_variant_type = 1;
select accurateCast(CAST(NULL, 'Variant(String, Int64)'), 'Nullable(Int32)') settings enable_variant_type = 1;
select accurateCast(CAST('abc', 'Dynamic'), 'Nullable(Int32)') settings enable_dynamic_type = 1;   -- { serverError CANNOT_PARSE_TEXT }

-- A NULL the source carries is returned whatever bytes its nested column holds.
select accurateCast(nullIf(materialize(1::Int64), 1), 'Nullable(Int8)');
select accurateCast(nullIf(materialize(999999999999::Int64), 999999999999), 'Nullable(Int8)');
select accurateCast(nullIf(materialize(1e39::Float64), 1e39), 'Nullable(Float32)');

-- Controls: the lenient conversions, the non-text sources and cast_ipv4_ipv6_default_on_conversion_error
-- are all outside the accurate text path and must keep their behaviour.
select CAST('abc', 'Nullable(Int32)');
select CAST('999999999999', 'Nullable(Int32)');
select CAST(nullIf(materialize(999999999999::Int64), 999999999999), 'Nullable(Int8)');
select accurateCastOrNull('abc', 'Int32');
select accurateCastOrNull(nullIf(materialize(999999999999::Int64), 999999999999), 'Int8');
select accurateCastOrDefault('abc', 'Int32');
select accurateCast(materialize(toIPv6('2001:db8::1')), 'Nullable(IPv4)');
select accurateCast('bad', 'Nullable(IPv4)') settings cast_ipv4_ipv6_default_on_conversion_error = 1;
