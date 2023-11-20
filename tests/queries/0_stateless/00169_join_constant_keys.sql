SELECT
    key1,
    key2,
    table_1
FROM
(
    SELECT
        arrayJoin([1, 2, 3]) AS key1,
        0 AS key2,
        999 AS table_1
) js1 ALL INNER JOIN
(
    SELECT
        arrayJoin([1, 3, 2]) AS key1,
        0 AS key2,
        999 AS table_1
) js2 USING key2, key1
ORDER BY key1;
