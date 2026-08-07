WITH [(1, 2)] AS arr1 SELECT arrayMap((x, y) -> (y, x), arr1);
WITH [(1, 2)] AS arr1 SELECT arrayMap(x -> x.1, arr1);
WITH [(1, 2)] AS arr1, [(3, 4)] AS arr2 SELECT arrayMap((x, y) -> (y.1, x.2), arr1, arr2);

WITH [(1, 2)] AS arr1 SELECT arrayMap((x, y, z) -> (y, x, z), arr1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
WITH [1, 2] AS arr1 SELECT arrayMap((x, y) -> (y, x), arr1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
