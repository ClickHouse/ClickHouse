SELECT flatten(arrayJoin([[[1, 2, 3], [4, 5]], [[6], [7, 8]]]));
SELECT arrayFlatten(arrayJoin([[[[]], [[1], [], [2, 3]]], [[[4]]]]));
SELECT flatten(arrayMap(x -> arrayMap(y -> arrayMap(z -> range(x), range(x)), range(x)), range(number))) FROM numbers(6);
SELECT flatten(arrayMap(x -> arrayMap(y -> arrayMap(z -> range(z), range(y)), range(x)), range(number))) FROM numbers(6);
SELECT flatten(arrayMap(x -> arrayMap(x -> arrayMap(x -> range(x), range(x)), range(x)), range(number))) FROM numbers(6) SETTINGS enable_analyzer=1;
SELECT arrayFlatten([[[1, 2, 3], [4, 5]], [[6], [7, 8]]]);
SELECT flatten([[[]]]);
SELECT arrayFlatten([]);
