SELECT bitmapContains(bitmapBuild([toUInt8(1)]), toUInt64(257));
SELECT bitmapContains(bitmapBuild([toUInt32(9)]), toUInt64(4294967305));
SELECT bitmapContains((SELECT groupBitmapState(toUInt32(number)) FROM numbers(1000)), toUInt64(4294967296));
SELECT bitmapContains(bitmapBuild([toUInt64(4294967296)]), toUInt64(4294967296));
