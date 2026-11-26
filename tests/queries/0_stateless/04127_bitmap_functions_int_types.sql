-- Exercise bitmap function type dispatch in Functions/FunctionsBitmap.h
-- across every integer element type (UInt8..UInt64, Int8..Int64).

SELECT '--- bitmapBuild + bitmapToArray: signed / unsigned element types ---';
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(UInt8)));
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(UInt16)));
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(UInt32)));
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(UInt64)));
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(Int8)));
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(Int16)));
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(Int32)));
SELECT bitmapToArray(bitmapBuild([1, 2, 3]::Array(Int64)));

SELECT '--- bitmapAnd / Or / Xor / Andnot ---';
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]::Array(UInt32)), bitmapBuild([2,3,4]::Array(UInt32))));
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2]::Array(UInt32)), bitmapBuild([3,4]::Array(UInt32))));
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]::Array(UInt32)), bitmapBuild([2,3,4]::Array(UInt32))));
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]::Array(UInt32)), bitmapBuild([2,3,4]::Array(UInt32))));

SELECT '--- bitmapSubsetInRange / bitmapSubsetLimit ---';
-- Element order inside the bitmap is an implementation detail; sort for stability.
SELECT arraySort(bitmapToArray(bitmapSubsetInRange(bitmapBuild([1,2,3,4,5,6]::Array(UInt32)), 2::UInt32, 5::UInt32)));
SELECT arraySort(bitmapToArray(bitmapSubsetLimit(bitmapBuild([1,2,3,4,5,6]::Array(UInt32)), 2::UInt32, 3::UInt32)));

SELECT '--- bitmapContains / bitmapHasAny / bitmapHasAll ---';
SELECT bitmapContains(bitmapBuild([1,2,3]::Array(UInt32)), 2::UInt32);
SELECT bitmapContains(bitmapBuild([1,2,3]::Array(UInt32)), 99::UInt32);
SELECT bitmapHasAny(bitmapBuild([1,2]::Array(UInt32)), bitmapBuild([2,3]::Array(UInt32)));
SELECT bitmapHasAny(bitmapBuild([1,2]::Array(UInt32)), bitmapBuild([10,20]::Array(UInt32)));
SELECT bitmapHasAll(bitmapBuild([1,2,3]::Array(UInt32)), bitmapBuild([1,2]::Array(UInt32)));
SELECT bitmapHasAll(bitmapBuild([1,2]::Array(UInt32)), bitmapBuild([1,2,3]::Array(UInt32)));

SELECT '--- cardinality helpers ---';
SELECT bitmapCardinality(bitmapBuild([1,2,3,4,5]::Array(UInt32)));
SELECT bitmapAndCardinality(bitmapBuild([1,2]::Array(UInt32)), bitmapBuild([2,3]::Array(UInt32)));
SELECT bitmapOrCardinality(bitmapBuild([1,2]::Array(UInt32)), bitmapBuild([2,3]::Array(UInt32)));
SELECT bitmapXorCardinality(bitmapBuild([1,2]::Array(UInt32)), bitmapBuild([2,3]::Array(UInt32)));
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]::Array(UInt32)), bitmapBuild([2,3,4]::Array(UInt32)));

SELECT '--- bitmapMin / Max ---';
SELECT bitmapMin(bitmapBuild([10,5,8]::Array(UInt32)));
SELECT bitmapMax(bitmapBuild([10,5,8]::Array(UInt32)));

SELECT '--- bitmapTransform ---';
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1,2,3]::Array(UInt32)), [1,2]::Array(UInt32), [10,20]::Array(UInt32)));
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1,2,3]::Array(UInt64)), [1,2]::Array(UInt64), [100,200]::Array(UInt64)));

SELECT '--- error: non-Array argument ---';
SELECT bitmapBuild('not array'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- error: non-integer element type ---';
SELECT bitmapBuild([1.5, 2.5]::Array(Float64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitmapBuild(['a', 'b']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- empty bitmap ---';
SELECT bitmapToArray(bitmapBuild([]::Array(UInt32)));
SELECT bitmapCardinality(bitmapBuild([]::Array(UInt32)));
