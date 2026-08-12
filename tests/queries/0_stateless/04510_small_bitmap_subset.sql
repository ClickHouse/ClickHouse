-- Small-set bitmaps store keys in insertion order; subBitmap must apply offset/limit in ascending order.

SELECT bitmapToArray(subBitmap(bitmapBuild([5, 4, 1, 2, 3]), 2, 2));
-- offset = 0
SELECT bitmapToArray(subBitmap(bitmapBuild([5, 4, 1, 2, 3]), 0, 2));
-- offset beyond size
SELECT bitmapToArray(subBitmap(bitmapBuild([5, 4, 1, 2, 3]), 10, 2));
-- offset and limit sum exceeds UInt64 max
SELECT bitmapToArray(subBitmap(bitmapBuild([1, 2, 3]), 1, 18446744073709551615));
-- negative signed bitmap
SELECT bitmapToArray(subBitmap(bitmapBuild([-1, 0, 1]::Array(Int32)), 0, 1));
-- negative signed bitmap for limit function
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([-1, 0, 1]::Array(Int32)), 0, 1));
-- values in bitmap exceeds UInt32 max
SELECT bitmapToArray(subBitmap(bitmapBuild([4294967297]::Array(UInt64)), 0, 1));
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([4294967297]::Array(UInt64)), 4294967297, 4294967298));
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([4294967297]::Array(UInt64)), 4294967297, 1));
-- signed Int8/Int16: compare as unsigned, so -1 matches the threshold and -128/-32768 do not
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([-128, -1]::Array(Int8)), 200, 1));
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([-128, -1]::Array(Int8)), 200, 256));
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([-32768, -1]::Array(Int16)), 40000, 1));
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([-32768, -1]::Array(Int16)), 40000, 65536));
-- same after promotion past the 32-element small set (Int8)
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild(arrayConcat(CAST(range(31), 'Array(Int8)'), [-128, -1]::Array(Int8))), 200, 1));
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild(arrayConcat(CAST(range(31), 'Array(Int8)'), [-128, -1]::Array(Int8))), 200, 256));
-- same after promotion (Int16)
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild(arrayConcat(CAST(range(31), 'Array(Int16)'), [-32768, -1]::Array(Int16))), 40000, 1));
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild(arrayConcat(CAST(range(31), 'Array(Int16)'), [-32768, -1]::Array(Int16))), 40000, 65536));
-- promoted UInt64: same value above UInt32 max
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild(arrayConcat(CAST(range(33), 'Array(UInt64)'), [4294967297]::Array(UInt64))), 4294967297, 4294967298));
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild(arrayConcat(CAST(range(33), 'Array(UInt64)'), [4294967297]::Array(UInt64))), 4294967297, 1));
-- promoted Int8 subBitmap: unsigned order is 0..30, -128, -1
SELECT bitmapToArray(subBitmap(bitmapBuild(arrayConcat(CAST(range(31), 'Array(Int8)'), [-128, -1]::Array(Int8))), 31, 1));
SELECT bitmapToArray(subBitmap(bitmapBuild(arrayConcat(CAST(range(31), 'Array(Int8)'), [-128, -1]::Array(Int8))), 32, 1));
-- promoted Int8: bitmapMin/Max/Contains use UnsignedT (not sign-extended UInt32 storage)
-- -128..-96 => unsigned 128..160
SELECT bitmapMin(bitmapBuild(arrayMap(x -> toInt8(x - 128), range(33))));
SELECT bitmapMax(bitmapBuild(arrayMap(x -> toInt8(x - 128), range(33))));
-- 33 elements, so the bitmap is promoted: the small set holds up to 32
SELECT bitmapContains(bitmapBuild(arrayConcat(CAST(range(32), 'Array(Int8)'), [-1]::Array(Int8))), 255);
SELECT bitmapContains(bitmapBuild(arrayConcat(CAST(range(32), 'Array(Int8)'), [-1]::Array(Int8))), 254);
-- promoted empty: bitmapMin must keep UINT32_MAX
SELECT bitmapMin(bitmapXor(bitmapBuild(CAST(range(33), 'Array(UInt8)')), bitmapBuild(CAST(range(33), 'Array(UInt8)'))));
SELECT bitmapMin(bitmapXor(bitmapBuild(CAST(range(33), 'Array(UInt16)')), bitmapBuild(CAST(range(33), 'Array(UInt16)'))));
SELECT bitmapMin(bitmapXor(bitmapBuild(CAST(range(33), 'Array(Int8)')), bitmapBuild(CAST(range(33), 'Array(Int8)'))));
SELECT bitmapMin(bitmapXor(bitmapBuild(CAST(range(33), 'Array(Int16)')), bitmapBuild(CAST(range(33), 'Array(Int16)'))));
-- bitmapTransform uses the same UnsignedT domain as bitmapContains
SELECT arraySort(bitmapToArray(bitmapTransform(bitmapBuild([-1, 0]::Array(Int8)), [255], [10])));
SELECT arraySort(bitmapToArray(bitmapTransform(bitmapBuild(arrayConcat(CAST(range(31), 'Array(Int8)'), [-1]::Array(Int8))), [255], [10])));
-- out-of-range mapping no-ops
SELECT arraySort(bitmapToArray(bitmapTransform(bitmapBuild([-1, 0]::Array(Int8)), [256], [10])));
