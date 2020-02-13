SELECT 
    [1, 2, 3, 1, 3] AS a, 
    indexOf(arrayReverse(arraySlice(a, 1, -1)), 3) AS offset_from_right, 
    arraySlice(a, multiIf(offset_from_right = 0, 1, (length(a) - offset_from_right) + 1));
