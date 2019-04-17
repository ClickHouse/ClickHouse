DROP TABLE IF EXISTS Test;

CREATE TABLE Test (
    createdDate Date,
    str String,
    key Enum8('A' = 0, 'B' = 1, 'ALL' = 2),
    a Int64
)
ENGINE = MergeTree(createdDate, str, 8192);

INSERT INTO Test VALUES ('2000-01-01', 'hello', 'A', 123);

SET max_threads = 1;

CREATE VIEW TestView AS
    SELECT str, key, sumIf(a, 0) AS sum
    FROM Test
    GROUP BY str, key

    UNION ALL

    SELECT str AS str, CAST('ALL' AS Enum8('A' = 0, 'B' = 1, 'ALL' = 2)) AS key, sumIf(a, 0) AS sum
    FROM Test
    GROUP BY str;

SELECT * FROM TestView;

DROP TABLE TestView;
DROP TABLE Test;
