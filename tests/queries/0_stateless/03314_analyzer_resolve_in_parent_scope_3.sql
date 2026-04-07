CREATE TABLE test
(
    a UInt64,
    b UInt64,
)
ENGINE = MergeTree
ORDER BY tuple();

WITH
   (a > b) as cte,
   query AS
    (
        SELECT count()
        FROM test
        WHERE cte
    )
SELECT *
FROM query;
