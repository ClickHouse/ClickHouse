DROP TABLE IF EXISTS Test_00584;

CREATE TABLE Test_00584 (
    createdDate Date,
    str String,
    key Enum8('A' = 0, 'B' = 1, 'ALL' = 2),
    a Int64
)
ENGINE = MergeTree(createdDate, str, 8192);

INSERT INTO Test_00584 VALUES ('2000-01-01', 'hello', 'A', 123);

SET max_threads = 1;

CREATE VIEW TestView AS
    SELECT str, key, sumIf(a, 0) AS sum
    FROM Test_00584
    GROUP BY str, key

    UNION ALL

    SELECT str AS str, CAST('ALL' AS Enum8('A' = 0, 'B' = 1, 'ALL' = 2)) AS key, sumIf(a, 0) AS sum
    FROM Test_00584
    GROUP BY str;

SELECT * FROM TestView ORDER BY key;

DROP TABLE TestView;
DROP TABLE Test_00584;
