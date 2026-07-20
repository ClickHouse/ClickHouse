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
