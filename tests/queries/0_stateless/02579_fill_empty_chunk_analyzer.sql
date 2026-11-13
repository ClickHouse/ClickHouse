-- this SELECT produces empty chunk in FillingTransform

SET enable_positional_arguments = 0;
SET enable_analyzer = 1;

-- With analyzer this special query has correct output
SELECT
    2 AS x,
    arrayJoin([NULL, NULL, NULL])
GROUP BY
    GROUPING SETS (
        (0),
        ([NULL, NULL, NULL]))
ORDER BY x ASC WITH FILL FROM 1 TO 10;
