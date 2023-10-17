SELECT sumForEach(arr), sumForEachIf(arr, arr[1] = 1), sumIfForEach(arr, arrayMap(x -> x != 5, arr)) FROM (SELECT arrayJoin([[1, 2, 3], [4, 5, 6]]) AS arr);
