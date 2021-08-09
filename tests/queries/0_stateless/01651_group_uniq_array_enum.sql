SELECT arraySort(groupUniqArray(x)) FROM (SELECT CAST(arrayJoin([1, 2, 3, 2, 3, 3]) AS Enum('Hello' = 1, 'World' = 2, 'Упячка' = 3)) AS x);
SELECT arraySort(groupArray(x)) FROM (SELECT CAST(arrayJoin([1, 2, 3, 2, 3, 3]) AS Enum('Hello' = 1, 'World' = 2, 'Упячка' = 3)) AS x);

SELECT
    arraySort(groupUniqArray(val)) AS uniq,
    toTypeName(uniq),
    arraySort(groupArray(val)) AS arr,
    toTypeName(arr)
FROM
(
    SELECT CAST(number % 2, 'Enum(\'hello\' = 1, \'world\' = 0)') AS val
    FROM numbers(2)
);
