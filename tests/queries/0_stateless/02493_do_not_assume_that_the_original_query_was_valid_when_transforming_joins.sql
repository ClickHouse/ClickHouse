CREATE TABLE table1 (column1 String) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE table2 (column1 String, column2 String, column3 String) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE table3 (column3 String) ENGINE=MergeTree() ORDER BY tuple();

SELECT
    *
FROM
(
    SELECT
        column1
    FROM table1
    GROUP BY
        column1
) AS a
ANY LEFT JOIN
(
    SELECT
        *
    FROM table2
) AS b ON (b.column1 = a.column1) AND (b.column2 = a.column2)
ANY LEFT JOIN
(
    SELECT
        *
    FROM table3
) AS c ON c.column3 = b.column3; -- {serverError UNKNOWN_IDENTIFIER}
