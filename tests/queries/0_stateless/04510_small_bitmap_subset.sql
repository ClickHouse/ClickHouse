-- Small-set bitmaps store keys in insertion order; subBitmap must apply offset/limit in ascending order.

SELECT bitmapToArray(subBitmap(bitmapBuild([5, 4, 1, 2, 3]), 2, 2));
-- offset = 0
SELECT bitmapToArray(subBitmap(bitmapBuild([5, 4, 1, 2, 3]), 0, 2));
-- offset beyond size
SELECT bitmapToArray(subBitmap(bitmapBuild([5, 4, 1, 2, 3]), 10, 2));