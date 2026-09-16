-- Exercise the exact-type numeric-array search path across inline-prefix, block, and tail boundaries.
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

SELECT 'UInt8 sizes',
    has(materialize(range(63)::Array(UInt8)), toUInt8(0)),
    indexOf(materialize(range(63)::Array(UInt8)), toUInt8(62)),
    has(materialize(range(64)::Array(UInt8)), toUInt8(63)),
    indexOf(materialize(range(64)::Array(UInt8)), toUInt8(63)),
    has(materialize(range(96)::Array(UInt8)), toUInt8(255)),
    indexOf(materialize(range(96)::Array(UInt8)), toUInt8(255)),
    has(materialize(range(128)::Array(UInt8)), toUInt8(0)),
    indexOf(materialize(range(128)::Array(UInt8)), toUInt8(0));

SELECT 'UInt16 sizes',
    has(materialize(range(31)::Array(UInt16)), toUInt16(0)),
    indexOf(materialize(range(31)::Array(UInt16)), toUInt16(30)),
    has(materialize(range(32)::Array(UInt16)), toUInt16(31)),
    indexOf(materialize(range(32)::Array(UInt16)), toUInt16(31)),
    has(materialize(range(48)::Array(UInt16)), toUInt16(65535)),
    indexOf(materialize(range(48)::Array(UInt16)), toUInt16(65535)),
    has(materialize(range(64)::Array(UInt16)), toUInt16(0)),
    indexOf(materialize(range(64)::Array(UInt16)), toUInt16(0));

SELECT 'UInt32 sizes',
    has(materialize(range(15)::Array(UInt32)), toUInt32(0)),
    indexOf(materialize(range(15)::Array(UInt32)), toUInt32(14)),
    has(materialize(range(16)::Array(UInt32)), toUInt32(15)),
    indexOf(materialize(range(16)::Array(UInt32)), toUInt32(15)),
    has(materialize(range(24)::Array(UInt32)), toUInt32(4294967295)),
    indexOf(materialize(range(24)::Array(UInt32)), toUInt32(4294967295)),
    has(materialize(range(32)::Array(UInt32)), toUInt32(0)),
    indexOf(materialize(range(32)::Array(UInt32)), toUInt32(0));

SELECT 'UInt64 sizes',
    has(materialize(range(31)::Array(UInt64)), toUInt64(0)),
    indexOf(materialize(range(31)::Array(UInt64)), toUInt64(30)),
    has(materialize(range(32)::Array(UInt64)), toUInt64(31)),
    indexOf(materialize(range(32)::Array(UInt64)), toUInt64(31)),
    has(materialize(range(36)::Array(UInt64)), toUInt64('18446744073709551615')),
    indexOf(materialize(range(36)::Array(UInt64)), toUInt64('18446744073709551615')),
    has(materialize(range(64)::Array(UInt64)), toUInt64(0)),
    indexOf(materialize(range(64)::Array(UInt64)), toUInt64(0));

SELECT 'UInt8 short',
    has(materialize(range(10)::Array(UInt8)), toUInt8(5)),
    indexOf(materialize(range(10)::Array(UInt8)), toUInt8(5)),
    has(materialize(range(10)::Array(UInt8)), toUInt8(9)),
    indexOf(materialize(range(10)::Array(UInt8)), toUInt8(9)),
    has(materialize(range(10)::Array(UInt8)), toUInt8(255)),
    indexOf(materialize(range(10)::Array(UInt8)), toUInt8(255));

-- Exercise matches near the scalar-prefix/continuation threshold for each supported width.

SELECT 'UInt8 threshold middle',
    has(materialize(range(64)::Array(UInt8)), toUInt8(16)),
    indexOf(materialize(range(64)::Array(UInt8)), toUInt8(16));

SELECT 'UInt16 threshold middle',
    has(materialize(range(32)::Array(UInt16)), toUInt16(8)),
    indexOf(materialize(range(32)::Array(UInt16)), toUInt16(8));

SELECT 'UInt32 threshold middle',
    has(materialize(range(16)::Array(UInt32)), toUInt32(4)),
    indexOf(materialize(range(16)::Array(UInt32)), toUInt32(4));

SELECT 'UInt64 threshold middle',
    has(materialize(range(32)::Array(UInt64)), toUInt64(2)),
    indexOf(materialize(range(32)::Array(UInt64)), toUInt64(2));

SELECT 'UInt8 threshold early',
    has(materialize(range(64)::Array(UInt8)), toUInt8(1)),
    indexOf(materialize(range(64)::Array(UInt8)), toUInt8(1));

SELECT 'UInt16 threshold early',
    has(materialize(range(32)::Array(UInt16)), toUInt16(1)),
    indexOf(materialize(range(32)::Array(UInt16)), toUInt16(1));

SELECT 'UInt32 threshold early',
    has(materialize(range(16)::Array(UInt32)), toUInt32(1)),
    indexOf(materialize(range(16)::Array(UInt32)), toUInt32(1));

SELECT 'UInt64 threshold early',
    has(materialize(range(32)::Array(UInt64)), toUInt64(1)),
    indexOf(materialize(range(32)::Array(UInt64)), toUInt64(1));

-- Exercise SIMD searches on rows with non-zero, changing array offsets.

SELECT length(arr),
    has(arr, toUInt8(32)),
    indexOf(arr, toUInt8(32)),
    has(arr, toUInt8(64)),
    indexOf(arr, toUInt8(64))
FROM
(
    SELECT arrayJoin([
        range(3)::Array(UInt8),
        range(64)::Array(UInt8),
        range(65)::Array(UInt8),
        range(67)::Array(UInt8)
    ]) AS arr
)
ORDER BY length(arr);

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
        range(19)::Array(UInt32),
        range(33)::Array(UInt32)
    ]) AS arr
)
ORDER BY length(arr);

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
        materialize(arrayMap(x -> if(x = 64 OR x = 68, toUInt32(777), toUInt32(x)), range(96))),
        toUInt32(777)),
    indexOf(
        materialize(arrayMap(x -> if(x = 64 OR x = 72, toUInt32(888), toUInt32(x)), range(96))),
        toUInt32(888));

-- Exercise indexOf's positive 16-value block rescan and the scalar tail after that block.

SELECT 'UInt16 continuation block',
    has(materialize(arrayMap(x -> if(x = 64 OR x = 68, toUInt16(777), toUInt16(x)), range(96))), toUInt16(777)),
    indexOf(materialize(arrayMap(x -> if(x = 64 OR x = 68, toUInt16(777), toUInt16(x)), range(96))), toUInt16(777)),
    has(materialize(range(81)::Array(UInt16)), toUInt16(80)),
    indexOf(materialize(range(81)::Array(UInt16)), toUInt16(80));

SELECT 'UInt32 continuation block',
    has(materialize(arrayMap(x -> if(x = 64 OR x = 68, toUInt32(777), toUInt32(x)), range(96))), toUInt32(777)),
    indexOf(materialize(arrayMap(x -> if(x = 64 OR x = 68, toUInt32(777), toUInt32(x)), range(96))), toUInt32(777)),
    has(materialize(range(81)::Array(UInt32)), toUInt32(80)),
    indexOf(materialize(range(81)::Array(UInt32)), toUInt32(80));

SELECT 'UInt64 continuation block',
    has(materialize(arrayMap(x -> if(x = 128 OR x = 132, toUInt64(777), toUInt64(x)), range(160))), toUInt64(777)),
    indexOf(materialize(arrayMap(x -> if(x = 128 OR x = 132, toUInt64(777), toUInt64(x)), range(160))), toUInt64(777)),
    has(materialize(range(145)::Array(UInt64)), toUInt64(144)),
    indexOf(materialize(range(145)::Array(UInt64)), toUInt64(144));

SELECT 'UInt8 long missing',
    has(materialize(arrayMap(x -> toUInt8(x % 255), range(4096))), toUInt8(255)),
    indexOf(materialize(arrayMap(x -> toUInt8(x % 255), range(4096))), toUInt8(255));
