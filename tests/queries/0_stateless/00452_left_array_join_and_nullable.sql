SELECT emptyArrayToSingle(arrayMap(x -> nullIf(x, 2), arrayJoin([emptyArrayUInt8(), [1], [2, 3]]))) AS arr;
SELECT arr, element FROM (SELECT arrayMap(x -> nullIf(x, 2), arrayJoin([emptyArrayUInt8(), [1], [2, 3]])) AS arr) LEFT ARRAY JOIN arr AS element;

SELECT emptyArrayToSingle(arr) FROM (SELECT arrayMap(x -> (x, toString(x), x = 1 ? NULL : x), range(number % 3)) AS arr FROM system.numbers LIMIT 10);

SELECT emptyArrayToSingle(arrayMap(x -> toString(x), arrayMap(x -> nullIf(x, 2), arrayJoin([emptyArrayUInt8(), [1], [2, 3]])))) AS arr;
SELECT emptyArrayToSingle(arrayMap(x -> toFixedString(toString(x), 3), arrayMap(x -> nullIf(x, 2), arrayJoin([emptyArrayUInt8(), [1], [2, 3], [3, 4, 5]])))) AS arr;
