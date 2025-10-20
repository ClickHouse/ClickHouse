DROP TABLE IF EXISTS tbl_x;

CREATE TABLE tbl_x (col2  String) ENGINE = Memory;

-- Produce Const and non-Const block in various SELECTs that may lead to UB w/o removing constness while squashing
INSERT INTO tbl_x
WITH
    c4 AS
    (
        SELECT 'aaa' AS col2
        UNION ALL
        SELECT 'bbb'
    ),
    c6 AS
    (
        SELECT r.col2 AS col2
        FROM (SELECT 'ccc' AS col2) AS r
        LEFT JOIN (SELECT 'foo' AS col2) AS rt
        USING col2
    )
SELECT
    *
FROM
(
    SELECT * FROM c4
    UNION ALL
    SELECT * FROM c6
);
