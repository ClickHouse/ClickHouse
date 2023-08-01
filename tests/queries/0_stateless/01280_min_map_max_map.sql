select minMap([toInt32(number % 10), number % 10 + 1], [number, 1]) as m, toTypeName(m) from numbers(1, 100);
select minMap([1], [toInt32(number) - 50]) from numbers(1, 100);
select minMap([cast(1, 'Decimal(10, 2)')], [cast(toInt32(number) - 50, 'Decimal(10, 2)')]) from numbers(1, 100);

select maxMap([toInt32(number % 10), number % 10 + 1], [number, 1]) as m, toTypeName(m) from numbers(1, 100);
select maxMap([1], [toInt32(number) - 50]) from numbers(1, 100);
select maxMap([cast(1, 'Decimal(10, 2)')], [cast(toInt32(number) - 50, 'Decimal(10, 2)')]) from numbers(1, 100);

-- check different types for minMap
select minMap(val, cnt) from values ('val Array(UUID), cnt Array(UUID)',
	(['01234567-89ab-cdef-0123-456789abcdef'], ['01111111-89ab-cdef-0123-456789abcdef']),
	(['01234567-89ab-cdef-0123-456789abcdef'], ['02222222-89ab-cdef-0123-456789abcdef']));
select minMap(val, cnt) from values ('val Array(String), cnt Array(String)',  (['1'], ['1']), (['1'], ['2']));
select minMap(val, cnt) from values ('val Array(FixedString(1)), cnt Array(FixedString(1))',  (['1'], ['1']), (['1'], ['2']));
select minMap(val, cnt) from values ('val Array(UInt64), cnt Array(UInt64)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val Array(Float64), cnt Array(Int8)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val Array(Date), cnt Array(Int16)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val Array(DateTime(\'Asia/Istanbul\')), cnt Array(Int32)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val Array(Decimal(10, 2)), cnt Array(Int16)',  (['1.01'], [1]), (['1.01'], [2]));
select minMap(val, cnt) from values ('val Array(Enum16(\'a\'=1)), cnt Array(Int16)',  (['a'], [1]), (['a'], [2]));

-- check different types for maxMap
select maxMap(val, cnt) from values ('val Array(UUID), cnt Array(UUID)',
	(['01234567-89ab-cdef-0123-456789abcdef'], ['01111111-89ab-cdef-0123-456789abcdef']),
	(['01234567-89ab-cdef-0123-456789abcdef'], ['02222222-89ab-cdef-0123-456789abcdef']));
select maxMap(val, cnt) from values ('val Array(String), cnt Array(String)',  (['1'], ['1']), (['1'], ['2']));
select maxMap(val, cnt) from values ('val Array(FixedString(1)), cnt Array(FixedString(1))',  (['1'], ['1']), (['1'], ['2']));
select maxMap(val, cnt) from values ('val Array(UInt64), cnt Array(UInt64)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val Array(Float64), cnt Array(Int8)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val Array(Date), cnt Array(Int16)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val Array(DateTime(\'Asia/Istanbul\')), cnt Array(Int32)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val Array(Decimal(10, 2)), cnt Array(Int16)',  (['1.01'], [1]), (['1.01'], [2]));
select maxMap(val, cnt) from values ('val Array(Enum16(\'a\'=1)), cnt Array(Int16)',  (['a'], [1]), (['a'], [2]));

-- bugfix, minMap and maxMap should not remove values with zero and empty strings but this behavior should not affect sumMap
select minMap(val, cnt) from values ('val Array(UInt64), cnt Array(UInt64)',  ([1], [0]), ([2], [0]));
select maxMap(val, cnt) from values ('val Array(UInt64), cnt Array(UInt64)',  ([1], [0]), ([2], [0]));
select minMap(val, cnt) from values ('val Array(String), cnt Array(String)',  (['A'], ['']), (['B'], ['']));
select maxMap(val, cnt) from values ('val Array(String), cnt Array(String)',  (['A'], ['']), (['B'], ['']));
select sumMap(val, cnt) from values ('val Array(UInt64), cnt Array(UInt64)',  ([1], [0]), ([2], [0]));

-- check working with arrays and tuples as values
select minMap([1, 1, 1], [[1, 2], [1], [1, 2, 3]]);
select maxMap([1, 1, 1], [[1, 2], [1], [1, 2, 3]]);
select minMap([1, 1, 1], [(1, 2), (1, 1), (1, 3)]);
select maxMap([1, 1, 1], [(1, 2), (1, 1), (1, 3)]);
