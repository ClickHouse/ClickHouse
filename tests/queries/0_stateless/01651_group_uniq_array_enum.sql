SELECT arraySort(groupUniqArray(x)) FROM (SELECT CAST(arrayJoin([1, 2, 3, 2, 3, 3]) AS Enum('Hello' = 1, 'World' = 2, 'Упячка' = 3)) AS x);
SELECT arraySort(groupArray(x)) FROM (SELECT CAST(arrayJoin([1, 2, 3, 2, 3, 3]) AS Enum('Hello' = 1, 'World' = 2, 'Упячка' = 3)) AS x);
