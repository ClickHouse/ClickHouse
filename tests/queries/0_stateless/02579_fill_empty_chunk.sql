-- this SELECT produces empty chunk in FillingTransform

SELECT
    2 AS x,
    arrayJoin([NULL, NULL, NULL])
GROUP BY
    GROUPING SETS (
        (0),
        ([NULL, NULL, NULL]))
ORDER BY x ASC WITH FILL FROM 1 TO 10;
