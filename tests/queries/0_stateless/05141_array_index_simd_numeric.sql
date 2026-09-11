-- Exercise the exact-type numeric-array search path around its vector threshold.
-- The same answers must hold for the first, last, and missing values across all supported widths.

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

SELECT 'UInt8 tail',
    has(materialize(range(65)::Array(UInt8)), toUInt8(64)),
    indexOf(materialize(range(65)::Array(UInt8)), toUInt8(64)),
    has(materialize(range(65)::Array(UInt8)), toUInt8(65)),
    indexOf(materialize(range(65)::Array(UInt8)), toUInt8(65));

SELECT 'UInt16 tail',
    has(materialize(range(65)::Array(UInt16)), toUInt16(64)),
    indexOf(materialize(range(65)::Array(UInt16)), toUInt16(64)),
    has(materialize(range(65)::Array(UInt16)), toUInt16(65)),
    indexOf(materialize(range(65)::Array(UInt16)), toUInt16(65));

SELECT 'UInt32 tail',
    has(materialize(range(65)::Array(UInt32)), toUInt32(64)),
    indexOf(materialize(range(65)::Array(UInt32)), toUInt32(64)),
    has(materialize(range(65)::Array(UInt32)), toUInt32(65)),
    indexOf(materialize(range(65)::Array(UInt32)), toUInt32(65));

SELECT 'UInt64 tail',
    has(materialize(range(65)::Array(UInt64)), toUInt64(64)),
    indexOf(materialize(range(65)::Array(UInt64)), toUInt64(64)),
    has(materialize(range(65)::Array(UInt64)), toUInt64(65)),
    indexOf(materialize(range(65)::Array(UInt64)), toUInt64(65));

SELECT 'UInt8 threshold',
    has(materialize(range(63)::Array(UInt8)), toUInt8(0)),
    indexOf(materialize(range(63)::Array(UInt8)), toUInt8(62)),
    has(materialize(range(64)::Array(UInt8)), toUInt8(63)),
    indexOf(materialize(range(64)::Array(UInt8)), toUInt8(63)),
    has(materialize(range(96)::Array(UInt8)), toUInt8(255)),
    indexOf(materialize(range(96)::Array(UInt8)), toUInt8(255)),
    has(materialize(range(128)::Array(UInt8)), toUInt8(0)),
    indexOf(materialize(range(128)::Array(UInt8)), toUInt8(0));

SELECT 'UInt16 threshold',
    has(materialize(range(31)::Array(UInt16)), toUInt16(0)),
    indexOf(materialize(range(31)::Array(UInt16)), toUInt16(30)),
    has(materialize(range(32)::Array(UInt16)), toUInt16(31)),
    indexOf(materialize(range(32)::Array(UInt16)), toUInt16(31)),
    has(materialize(range(48)::Array(UInt16)), toUInt16(65535)),
    indexOf(materialize(range(48)::Array(UInt16)), toUInt16(65535)),
    has(materialize(range(64)::Array(UInt16)), toUInt16(0)),
    indexOf(materialize(range(64)::Array(UInt16)), toUInt16(0));

SELECT 'UInt32 threshold',
    has(materialize(range(15)::Array(UInt32)), toUInt32(0)),
    indexOf(materialize(range(15)::Array(UInt32)), toUInt32(14)),
    has(materialize(range(16)::Array(UInt32)), toUInt32(15)),
    indexOf(materialize(range(16)::Array(UInt32)), toUInt32(15)),
    has(materialize(range(24)::Array(UInt32)), toUInt32(4294967295)),
    indexOf(materialize(range(24)::Array(UInt32)), toUInt32(4294967295)),
    has(materialize(range(32)::Array(UInt32)), toUInt32(0)),
    indexOf(materialize(range(32)::Array(UInt32)), toUInt32(0));

SELECT 'UInt64 threshold',
    has(materialize(range(31)::Array(UInt64)), toUInt64(0)),
    indexOf(materialize(range(31)::Array(UInt64)), toUInt64(30)),
    has(materialize(range(32)::Array(UInt64)), toUInt64(31)),
    indexOf(materialize(range(32)::Array(UInt64)), toUInt64(31)),
    has(materialize(range(36)::Array(UInt64)), toUInt64('18446744073709551615')),
    indexOf(materialize(range(36)::Array(UInt64)), toUInt64('18446744073709551615')),
    has(materialize(range(64)::Array(UInt64)), toUInt64(0)),
    indexOf(materialize(range(64)::Array(UInt64)), toUInt64(0));

SELECT 'UInt16 lane equality',
    has(materialize(arrayMap(x -> toUInt16(257), range(64))), toUInt16(1)),
    indexOf(materialize(arrayMap(x -> toUInt16(257), range(64))), toUInt16(1)),
    has(materialize(arrayMap(x -> toUInt16(257), range(64))), toUInt16(257)),
    indexOf(materialize(arrayMap(x -> toUInt16(257), range(64))), toUInt16(257));

SELECT 'UInt32 lane equality',
    has(materialize(arrayMap(x -> toUInt32(16777217), range(64))), toUInt32(1)),
    indexOf(materialize(arrayMap(x -> toUInt32(16777217), range(64))), toUInt32(1)),
    has(materialize(arrayMap(x -> toUInt32(16777217), range(64))), toUInt32(16777217)),
    indexOf(materialize(arrayMap(x -> toUInt32(16777217), range(64))), toUInt32(16777217));

SELECT 'UInt64 lane equality',
    has(materialize(arrayMap(x -> toUInt64(72057594037927937), range(64))), toUInt64(1)),
    indexOf(materialize(arrayMap(x -> toUInt64(72057594037927937), range(64))), toUInt64(1)),
    has(materialize(arrayMap(x -> toUInt64(72057594037927937), range(64))), toUInt64(72057594037927937)),
    indexOf(materialize(arrayMap(x -> toUInt64(72057594037927937), range(64))), toUInt64(72057594037927937));

SELECT 'Unsigned max SIMD',
    has(materialize(arrayMap(x -> if(x = 40, toUInt8(255), toUInt8(x)), range(64))), toUInt8(255)),
    indexOf(materialize(arrayMap(x -> if(x = 40, toUInt8(255), toUInt8(x)), range(64))), toUInt8(255)),
    has(materialize(arrayMap(x -> if(x = 20, toUInt16(65535), toUInt16(x)), range(32))), toUInt16(65535)),
    indexOf(materialize(arrayMap(x -> if(x = 20, toUInt16(65535), toUInt16(x)), range(32))), toUInt16(65535)),
    has(materialize(arrayMap(x -> if(x = 12, toUInt32(4294967295), toUInt32(x)), range(32))), toUInt32(4294967295)),
    indexOf(materialize(arrayMap(x -> if(x = 12, toUInt32(4294967295), toUInt32(x)), range(32))), toUInt32(4294967295)),
    has(materialize(arrayMap(x -> if(x = 8, toUInt64('18446744073709551615'), toUInt64(x)), range(32))), toUInt64('18446744073709551615')),
    indexOf(materialize(arrayMap(x -> if(x = 8, toUInt64('18446744073709551615'), toUInt64(x)), range(32))), toUInt64('18446744073709551615'));

SELECT 'UInt32 duplicate SIMD',
    indexOf(
        materialize(arrayMap(x -> if(x = 16 OR x = 20, toUInt32(777), toUInt32(x)), range(64))),
        toUInt32(777)),
    indexOf(
        materialize(arrayMap(x -> if(x = 16 OR x = 24, toUInt32(888), toUInt32(x)), range(64))),
        toUInt32(888));

SELECT 'UInt8 long missing',
    has(materialize(arrayMap(x -> toUInt8(x % 255), range(4096))), toUInt8(255)),
    indexOf(materialize(arrayMap(x -> toUInt8(x % 255), range(4096))), toUInt8(255));
