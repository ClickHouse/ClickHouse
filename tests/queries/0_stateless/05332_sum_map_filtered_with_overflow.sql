-- Test `sumMapFilteredWithOverflow` and the single tuple argument form of `sumMapFiltered`
SELECT sumMapFiltered([1, 3])((k, v)) AS r, toTypeName(r) FROM values('k Array(UInt8), v Array(UInt8)', ([1, 2, 3], [255, 1, 1]), ([1, 2], [2, 1]));
SELECT sumMapFilteredWithOverflow([1, 3])(k, v) AS r, toTypeName(r), sumMapFilteredWithOverflow([1, 3])((k, v)) FROM values('k Array(UInt8), v Array(UInt8)', ([1, 2, 3], [255, 1, 1]), ([1, 2], [2, 1]));
