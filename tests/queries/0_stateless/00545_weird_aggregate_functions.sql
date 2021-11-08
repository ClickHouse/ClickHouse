SELECT sumForEachMergeArray(y) FROM (SELECT sumForEachStateForEachIfArrayIfMerge(x) AS y FROM (SELECT sumForEachStateForEachIfArrayIfState([[[1, 2, 3], [4, 5, 6], [7, 8, 9]]], [1], 1) AS x));
