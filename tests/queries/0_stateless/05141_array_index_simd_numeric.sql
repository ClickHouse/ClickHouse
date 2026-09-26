-- Exercise constant numeric needles across the optimized numeric search path.

SELECT 'UInt8',
    has(materialize(range(64)::Array(UInt8)), toUInt8(0)),
    has(materialize(range(64)::Array(UInt8)), toUInt8(63)),
    has(materialize(range(64)::Array(UInt8)), toUInt8(64)),
    indexOf(materialize(range(64)::Array(UInt8)), toUInt8(0)),
    indexOf(materialize(range(64)::Array(UInt8)), toUInt8(63)),
    indexOf(materialize(range(64)::Array(UInt8)), toUInt8(64));

SELECT 'UInt16',
    has(materialize(range(64)::Array(UInt16)), toUInt16(0)),
    has(materialize(range(64)::Array(UInt16)), toUInt16(63)),
    has(materialize(range(64)::Array(UInt16)), toUInt16(64)),
    indexOf(materialize(range(64)::Array(UInt16)), toUInt16(0)),
    indexOf(materialize(range(64)::Array(UInt16)), toUInt16(63)),
    indexOf(materialize(range(64)::Array(UInt16)), toUInt16(64));

SELECT 'UInt32',
    has(materialize(range(64)::Array(UInt32)), toUInt32(0)),
    has(materialize(range(64)::Array(UInt32)), toUInt32(63)),
    has(materialize(range(64)::Array(UInt32)), toUInt32(64)),
    indexOf(materialize(range(64)::Array(UInt32)), toUInt32(0)),
    indexOf(materialize(range(64)::Array(UInt32)), toUInt32(63)),
    indexOf(materialize(range(64)::Array(UInt32)), toUInt32(64));

SELECT 'UInt64',
    has(materialize(range(64)::Array(UInt64)), toUInt64(0)),
    has(materialize(range(64)::Array(UInt64)), toUInt64(63)),
    has(materialize(range(64)::Array(UInt64)), toUInt64(64)),
    indexOf(materialize(range(64)::Array(UInt64)), toUInt64(0)),
    indexOf(materialize(range(64)::Array(UInt64)), toUInt64(63)),
    indexOf(materialize(range(64)::Array(UInt64)), toUInt64(64));

SELECT 'UInt8 short',
    has(materialize(range(10)::Array(UInt8)), toUInt8(5)),
    indexOf(materialize(range(10)::Array(UInt8)), toUInt8(5)),
    has(materialize(range(10)::Array(UInt8)), toUInt8(255)),
    indexOf(materialize(range(10)::Array(UInt8)), toUInt8(255));

SELECT 'Int8 byte',
    has(materialize([toInt8(-1), toInt8(0), toInt8(1)]), toInt8(-1)),
    indexOf(materialize([toInt8(-1), toInt8(0), toInt8(1)]), toInt8(-1)),
    has(materialize([toInt8(-1), toInt8(0), toInt8(1)]), toInt8(-2)),
    indexOf(materialize([toInt8(-1), toInt8(0), toInt8(1)]), toInt8(-2));

SELECT 'Signed integers',
    has(materialize(range(64)::Array(Int16)), toInt16(0)),
    indexOf(materialize(range(64)::Array(Int16)), toInt16(63)),
    has(materialize(range(64)::Array(Int32)), toInt32(0)),
    indexOf(materialize(range(64)::Array(Int32)), toInt32(63)),
    has(materialize(range(64)::Array(Int64)), toInt64(0)),
    indexOf(materialize(range(64)::Array(Int64)), toInt64(63));

SELECT 'Float32',
    has(materialize(arrayMap(x -> toFloat32(x), range(64))), toFloat32(0)),
    indexOf(materialize(arrayMap(x -> toFloat32(x), range(64))), toFloat32(63)),
    has(materialize(arrayMap(x -> toFloat32(x), range(64))), toFloat32(64)),
    indexOf(materialize(arrayMap(x -> toFloat32(x), range(64))), toFloat32(64));

SELECT 'Float64',
    has(materialize(arrayMap(x -> toFloat64(x), range(64))), toFloat64(0)),
    indexOf(materialize(arrayMap(x -> toFloat64(x), range(64))), toFloat64(63)),
    has(materialize(arrayMap(x -> toFloat64(x), range(64))), toFloat64(64)),
    indexOf(materialize(arrayMap(x -> toFloat64(x), range(64))), toFloat64(64));

SELECT 'Exact numeric conversion',
    has(materialize(range(64)::Array(UInt8)), toInt32(300)),
    indexOf(materialize(range(64)::Array(UInt8)), toInt32(300)),
    has(materialize(arrayMap(x -> toFloat32(x), range(64))), toFloat64(16777217)),
    indexOf(materialize(arrayMap(x -> toFloat32(x), range(64))), toFloat64(16777217)),
    has(materialize(range(64)::Array(Int32)), nan),
    indexOf(materialize(range(64)::Array(Int32)), nan);

SELECT 'NaN needle',
    has(materialize([toFloat32(nan)]), toFloat32(nan)),
    indexOf(materialize([toFloat32(nan)]), toFloat32(nan)),
    has(materialize([toFloat64(nan)]), toFloat64(nan)),
    indexOf(materialize([toFloat64(nan)]), toFloat64(nan));

SELECT 'Bare UInt32 max',
    has(materialize(arrayMap(x -> if(x = 12, toUInt32(4294967295), toUInt32(x)), range(32))), 4294967295),
    indexOf(materialize(arrayMap(x -> if(x = 12, toUInt32(4294967295), toUInt32(x)), range(32))), 4294967295);

SELECT 'Bare UInt64 max',
    has(materialize(arrayMap(x -> if(x = 8, toUInt64('18446744073709551615'), toUInt64(x)), range(32))), 18446744073709551615),
    indexOf(materialize(arrayMap(x -> if(x = 8, toUInt64('18446744073709551615'), toUInt64(x)), range(32))), 18446744073709551615);

SELECT length(arr),
    has(arr, toUInt32(8)),
    indexOf(arr, toUInt32(8)),
    has(arr, toUInt32(16)),
    indexOf(arr, toUInt32(16))
FROM
(
    SELECT arrayJoin([
        range(3)::Array(UInt32),
        range(16)::Array(UInt32),
        range(17)::Array(UInt32),
        range(33)::Array(UInt32)
    ]) AS arr
)
ORDER BY length(arr);

SELECT 'First duplicate',
    has(
        materialize(arrayMap(x -> if(x = 16 OR x = 20, toUInt32(777), toUInt32(x)), range(96))),
        toUInt32(777)),
    indexOf(
        materialize(arrayMap(x -> if(x = 16 OR x = 20, toUInt32(777), toUInt32(x)), range(96))),
        toUInt32(777));

SELECT 'UInt32 tail',
    has(materialize(range(65)::Array(UInt32)), toUInt32(64)),
    indexOf(materialize(range(65)::Array(UInt32)), toUInt32(64)),
    has(materialize(range(65)::Array(UInt32)), toUInt32(65)),
    indexOf(materialize(range(65)::Array(UInt32)), toUInt32(65));

SELECT 'UInt8 long missing',
    has(materialize(arrayMap(x -> toUInt8(x % 255), range(4096))), toUInt8(255)),
    indexOf(materialize(arrayMap(x -> toUInt8(x % 255), range(4096))), toUInt8(255));
