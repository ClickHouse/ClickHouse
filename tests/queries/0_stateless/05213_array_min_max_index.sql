SELECT arrayMinIndex([5, 3, 2, 7]), arrayMaxIndex([5, 3, 2, 7]);
SELECT arrayMinIndex([5, 3, 3, 7]), arrayMaxIndex([5, 7, 7, 3]);
SELECT arrayMinIndex(emptyArrayInt32()), arrayMaxIndex(emptyArrayUInt64());
SELECT arrayMinIndex([42]), arrayMaxIndex([42]);
SELECT arrayMinIndex(['b', 'a', 'a']), arrayMaxIndex(['b', 'a', 'a']);
SELECT arrayMinIndex(x -> abs(x), [-10, 7, 3]), arrayMaxIndex(x -> abs(x), [-10, 7, 3]);
SELECT arrayMinIndex(x -> 1, [1, 2, 3]), arrayMaxIndex(x -> 1, [1, 2, 3]);
SELECT arrayMinIndex(x, y -> x * y, [1, 5, 3], [2, 2, 5]), arrayMaxIndex(x, y -> x * y, [1, 5, 3], [2, 2, 5]);
SELECT arrayMinIndex([nan, 2.0, 1.0]), arrayMaxIndex([nan, 2.0, 1.0]);
SELECT arrayMinIndex([nan, nan]), arrayMaxIndex([nan, nan]);
SELECT arrayMinIndex([0.0, -0.0]), arrayMaxIndex([0.0, -0.0]);
SELECT arrayMinIndex(range(128)), arrayMaxIndex(range(128));
SELECT arrayMinIndex(range(16384)), arrayMaxIndex(range(16384));
SELECT arrayMinIndex(a), arrayMaxIndex(a)
FROM (SELECT arrayJoin([range(128), arrayReverse(range(128)), range(64)]) AS a)
ORDER BY length(a), arrayMax(a);
SELECT
    n,
    arrayMinIndex(arrayMap(i -> if(i < 2, 100, if(i >= n - 2, 0, 50)), range(n))),
    arrayMaxIndex(arrayMap(i -> if(i < 2, 100, if(i >= n - 2, 0, 50)), range(n)))
FROM (SELECT arrayJoin([48, 49, 64, 65, 256, 257, 16383, 16384, 16385]) AS n)
ORDER BY n;
SELECT arrayMinIndex(arrayMap(x -> nan, range(16384))), arrayMaxIndex(arrayMap(x -> nan, range(16384)));
SELECT
    arrayMinIndex(arrayConcat(arrayMap(x -> nan, range(1024)), arrayMap(x -> toFloat64(x), range(15360)))),
    arrayMaxIndex(arrayConcat(arrayMap(x -> nan, range(1024)), arrayMap(x -> toFloat64(x), range(15360))));
SELECT arrayMinIndex([NULL, 2, 1]), arrayMaxIndex([NULL, 2, 1]);
SELECT arrayMinIndex([NULL::Nullable(Int64), NULL::Nullable(Int64)]), arrayMaxIndex([NULL::Nullable(Int64), NULL::Nullable(Int64)]);
SELECT arrayMinIndex(x -> 0, emptyArrayInt32()), arrayMaxIndex(x -> 0, emptyArrayInt32());
SELECT arrayMinIndex([toUInt8(255), toUInt8(0), toUInt8(0), toUInt8(1)]), arrayMaxIndex([toUInt8(1), toUInt8(255), toUInt8(255), toUInt8(0)]);
SELECT arrayMinIndex([toUInt16(65535), toUInt16(0), toUInt16(0), toUInt16(1)]), arrayMaxIndex([toUInt16(1), toUInt16(65535), toUInt16(65535), toUInt16(0)]);
SELECT arrayMinIndex([toUInt32(4294967295), toUInt32(0), toUInt32(0), toUInt32(1)]), arrayMaxIndex([toUInt32(1), toUInt32(4294967295), toUInt32(4294967295), toUInt32(0)]);
SELECT arrayMinIndex([toUInt64('18446744073709551615'), toUInt64(0), toUInt64(0), toUInt64(1)]), arrayMaxIndex([toUInt64(1), toUInt64('18446744073709551615'), toUInt64('18446744073709551615'), toUInt64(0)]);
SELECT arrayMinIndex([toInt8(-1), toInt8(-128), toInt8(-128), toInt8(0)]), arrayMaxIndex([toInt8(-1), toInt8(127), toInt8(127), toInt8(0)]);
SELECT arrayMinIndex([toInt16(1), toInt16(-32768), toInt16(-32768), toInt16(0)]), arrayMaxIndex([toInt16(-1), toInt16(32767), toInt16(32767), toInt16(0)]);
SELECT arrayMinIndex([toInt32(1), toInt32(-2147483648), toInt32(-2147483648), toInt32(0)]), arrayMaxIndex([toInt32(-1), toInt32(2147483647), toInt32(2147483647), toInt32(0)]);
SELECT arrayMinIndex([toInt64(1), toInt64('-9223372036854775807'), toInt64('-9223372036854775807'), toInt64(0)]), arrayMaxIndex([toInt64(-1), toInt64('9223372036854775807'), toInt64('9223372036854775807'), toInt64(0)]);
SELECT
    arrayMinIndex([toInt128(0), toInt128(-1), toInt128(-1), toInt128(1), toInt128(1)]),
    arrayMaxIndex([toInt128(0), toInt128(-1), toInt128(-1), toInt128(1), toInt128(1)]),
    arrayMinIndex([toUInt128(0), toUInt128(2), toUInt128(2), toUInt128(1), toUInt128(1)]),
    arrayMaxIndex([toUInt128(0), toUInt128(2), toUInt128(2), toUInt128(1), toUInt128(1)]),
    arrayMinIndex([toInt256(0), toInt256(-1), toInt256(-1), toInt256(1), toInt256(1)]),
    arrayMaxIndex([toInt256(0), toInt256(-1), toInt256(-1), toInt256(1), toInt256(1)]),
    arrayMinIndex([toUInt256(0), toUInt256(2), toUInt256(2), toUInt256(1), toUInt256(1)]),
    arrayMaxIndex([toUInt256(0), toUInt256(2), toUInt256(2), toUInt256(1), toUInt256(1)]);
SELECT arrayMinIndex([nan::Float32, -inf::Float32, -inf::Float32, 0::Float32]), arrayMaxIndex([nan::Float32, inf::Float32, inf::Float32, 0::Float32]);
SELECT arrayMinIndex([nan::Float64, -inf::Float64, -inf::Float64, 0::Float64]), arrayMaxIndex([nan::Float64, inf::Float64, inf::Float64, 0::Float64]);
SELECT arrayMinIndex([(2, 'b'), (1, 'c'), (1, 'a')]), arrayMaxIndex([(2, 'b'), (1, 'c'), (1, 'a')]);
SELECT arrayMinIndex([toDecimal32(2, 2), toDecimal32(1, 2), toDecimal32(1, 2)]), arrayMaxIndex([toDecimal32(2, 2), toDecimal32(1, 2), toDecimal32(1, 2)]);
SELECT arrayMinIndex([toDate('2024-01-02'), toDate('2024-01-01'), toDate('2024-01-01')]), arrayMaxIndex([toDate('2024-01-02'), toDate('2024-01-01'), toDate('2024-01-01')]);
SELECT arrayMinIndex(x -> length(x), ['aaa', 'b', 'b']), arrayMaxIndex(x -> length(x), ['aaa', 'b', 'b']);
SELECT arrayMinIndex(x, y -> x + y, [3, 1, 1, 5], [1, 4, 4, 0]), arrayMaxIndex(x, y -> x + y, [3, 1, 1, 5], [1, 4, 4, 0]);
SELECT
    n,
    arrayMaxIndex(arrayMap(i -> if(i = 0, toInt64(100000), toInt64(i)), range(n))),
    arrayMaxIndex(arrayMap(i -> if(i = intDiv(n, 2), toInt64(100000), toInt64(i)), range(n))),
    arrayMaxIndex(arrayMap(i -> if(i = n - 1, toInt64(100000), toInt64(i)), range(n))),
    arrayMinIndex(arrayMap(i -> if(i = 0, toInt64(-100000), toInt64(i)), range(n))),
    arrayMinIndex(arrayMap(i -> if(i = intDiv(n, 2), toInt64(-100000), toInt64(i)), range(n))),
    arrayMinIndex(arrayMap(i -> if(i = n - 1, toInt64(-100000), toInt64(i)), range(n)))
FROM (SELECT arrayJoin([1, 2, 3, 7, 8, 15, 16, 31, 32, 47, 48, 49, 63, 64, 65, 95, 96, 127, 128, 129, 191, 192, 255, 256, 257, 511, 512, 513, 1023, 1024, 2047, 2048, 4095, 4096, 8191, 8192, 16383, 16384, 16385, 32768]) AS n)
ORDER BY n;
SELECT n, arrayMinIndex(arrayMap(x -> nan, range(n))), arrayMaxIndex(arrayMap(x -> nan, range(n)))
FROM (SELECT arrayJoin([16384, 16385, 32768, 32769, 65536]) AS n)
ORDER BY n;
SELECT
    arrayMinIndex(arrayConcat(arrayMap(x -> toFloat64(x), range(1024)), arrayMap(x -> nan, range(1024)), arrayMap(x -> toFloat64(x), range(15360)))),
    arrayMaxIndex(arrayConcat(arrayMap(x -> toFloat64(x), range(1024)), arrayMap(x -> nan, range(1024)), arrayMap(x -> toFloat64(x), range(15360))));
SELECT arrayMinIndex(arrayReverse(range(65536))), arrayMaxIndex(arrayReverse(range(65536)));
SELECT arrayMinIndex(arrayConcat([toUInt64(0), toUInt64(0)], range(16384))), arrayMaxIndex(arrayConcat([toUInt64(0), toUInt64(0)], range(16384)));
SELECT arrayMinIndex(a), arrayMaxIndex(a)
FROM (SELECT arrayJoin([range(65), arrayReverse(range(66)), range(128), arrayReverse(range(256)), range(257)]) AS a)
ORDER BY length(a);
SELECT
    arrayMinIndex(arrayMap(x -> toUInt64(42), range(16384))),
    arrayMaxIndex(arrayMap(x -> toUInt64(42), range(16384))),
    arrayMinIndex(arrayMap(x -> toFloat64(42), range(16384))),
    arrayMaxIndex(arrayMap(x -> toFloat64(42), range(16384)));
SELECT
    n,
    arrayMaxIndex(arrayMap(i -> if(i = 0, toInt64(100000), toInt64(i)), range(n))),
    arrayMaxIndex(arrayMap(i -> if(i = intDiv(n, 2), toInt64(100000), toInt64(i)), range(n))),
    arrayMaxIndex(arrayMap(i -> if(i = n - 1, toInt64(100000), toInt64(i)), range(n))),
    arrayMaxIndex(arrayMap(i -> if(i = intDiv(n * 3, 5), toInt64(100000), toInt64(i)), range(n))),
    arrayMinIndex(arrayMap(i -> if(i = 0, toInt64(-100000), toInt64(i)), range(n))),
    arrayMinIndex(arrayMap(i -> if(i = intDiv(n, 2), toInt64(-100000), toInt64(i)), range(n))),
    arrayMinIndex(arrayMap(i -> if(i = n - 1, toInt64(-100000), toInt64(i)), range(n))),
    arrayMinIndex(arrayMap(i -> if(i = intDiv(n * 3, 5), toInt64(-100000), toInt64(i)), range(n)))
FROM (SELECT arrayJoin(range(1, 129)) AS n)
ORDER BY n;
SELECT
    arrayMinIndex(arrayMap(i -> if(i = 8192, toUInt8(0), toUInt8(i % 200 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toUInt8(255), toUInt8(i % 200)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toUInt16(0), toUInt16(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toUInt16(65535), toUInt16(i % 20000)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toUInt32(0), toUInt32(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toUInt32(4294967295), toUInt32(i % 20000)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toUInt64(0), toUInt64(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toUInt64('18446744073709551615'), toUInt64(i % 20000)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toInt8(-128), toInt8(i % 100 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toInt8(127), toInt8(i % 100)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toInt16(-32768), toInt16(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toInt16(32767), toInt16(i % 20000)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toInt32(-2147483648), toInt32(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toInt32(2147483647), toInt32(i % 20000)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toInt64('-9223372036854775807'), toInt64(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toInt64('9223372036854775807'), toInt64(i % 20000)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toFloat32('-1000000000000'), toFloat32(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toFloat32('1000000000000'), toFloat32(i % 20000)), range(16384))),
    arrayMinIndex(arrayMap(i -> if(i = 8192, toFloat64('-1000000000000'), toFloat64(i % 20000 + 1)), range(16384))),
    arrayMaxIndex(arrayMap(i -> if(i = 8192, toFloat64('1000000000000'), toFloat64(i % 20000)), range(16384)));
