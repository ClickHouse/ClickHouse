-- Binary bitmap functions over states of different versions return the newer version
-- regardless of the order of the arguments.

WITH
    CAST(bitmapBuild([1, 2, 3]::Array(UInt32)) AS AggregateFunction(0, groupBitmap, UInt32)) AS v0,
    CAST(bitmapBuild([3, 4]::Array(UInt32)) AS AggregateFunction(1, groupBitmap, UInt32)) AS v1
SELECT
    toTypeName(bitmapAnd(v0, v1)) = toTypeName(bitmapAnd(v1, v0)),
    toTypeName(bitmapOr(v0, v1)) = toTypeName(v1),
    toTypeName(bitmapXor(v1, v0)) = toTypeName(v1),
    toTypeName(bitmapAndnot(v0, v1)) = toTypeName(v1),
    arraySort(bitmapToArray(bitmapAnd(v0, v1))),
    arraySort(bitmapToArray(bitmapOr(v1, v0))),
    arraySort(bitmapToArray(bitmapXor(v0, v1))),
    arraySort(bitmapToArray(bitmapAndnot(v0, v1)));

DROP TABLE IF EXISTS t_bitmap_mixed_versions;
CREATE TABLE t_bitmap_mixed_versions (v0 AggregateFunction(0, groupBitmap, UInt32), v1 AggregateFunction(1, groupBitmap, UInt32)) ENGINE = Memory;
INSERT INTO t_bitmap_mixed_versions SELECT bitmapBuild([1, 2, 3]::Array(UInt32)), bitmapBuild([3, 4]::Array(UInt32));
SELECT toTypeName(bitmapOr(v0, v1)), toTypeName(bitmapOr(v1, v0)), arraySort(bitmapToArray(bitmapOr(v0, v1))), arraySort(bitmapToArray(bitmapOr(v1, v0))) FROM t_bitmap_mixed_versions;
DROP TABLE t_bitmap_mixed_versions;
