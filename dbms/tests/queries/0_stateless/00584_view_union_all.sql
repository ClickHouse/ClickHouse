DROP TABLE IF EXISTS test.Test;

CREATE TABLE test.Test (
    createdDate Date,
    str String,
    key Enum8('A' = 0, 'B' = 1, 'ALL' = 2),
    a Int64
)
ENGINE = MergeTree(createdDate, str, 8192);

INSERT INTO test.Test VALUES ('2000-01-01', 'hello', 'A', 123);

SET max_threads = 1;

CREATE VIEW test.TestView AS
    SELECT str, key, sumIf(a, 0) AS sum
    FROM test.Test
    GROUP BY str, key

    UNION ALL

    SELECT str AS str, CAST('ALL' AS Enum8('A' = 0, 'B' = 1, 'ALL' = 2)) AS key, sumIf(a, 0) AS sum
    FROM test.Test
    GROUP BY str;

SELECT * FROM test.TestView;

DROP TABLE test.TestView;
DROP TABLE test.Test;
