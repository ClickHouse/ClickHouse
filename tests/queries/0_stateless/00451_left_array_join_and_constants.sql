SELECT arr, element FROM (SELECT [1] AS arr) LEFT ARRAY JOIN arr AS element;
SELECT arr, element FROM (SELECT emptyArrayUInt8() AS arr) LEFT ARRAY JOIN arr AS element;
SELECT arr, element FROM (SELECT arrayJoin([emptyArrayUInt8(), [1], [2, 3]]) AS arr) LEFT ARRAY JOIN arr AS element;
