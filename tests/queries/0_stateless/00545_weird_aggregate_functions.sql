SELECT sumForEachMergeArray(y) FROM (SELECT sumForEachStateForEachIfArrayMerge(x) AS y FROM (SELECT sumForEachStateForEachIfArrayState([[[1, 2, 3], [4, 5, 6], [7, 8, 9]]], [1]) AS x));
