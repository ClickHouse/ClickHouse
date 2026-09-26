-- { echoOn }

SELECT
    ntile(1) OVER (ORDER BY id ASC) AS a,
    ntile(2) OVER (ORDER BY id ASC) AS b
FROM
(
    SELECT 1 AS id
) AS t;

SELECT
    ntile(1) OVER (ORDER BY id ASC) AS a,
    ntile(2) OVER (ORDER BY id + 0 ASC) AS b
FROM
(
    SELECT 1 AS id
) AS t;

SELECT
    ntile(1) OVER (ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS a,
    ntile(2) OVER (ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS b
FROM
(
    SELECT 1 AS id
) AS t;


DROP TABLE IF EXISTS test_ntile;

CREATE TABLE test_ntile
(
    id Int32
)
ENGINE = Memory;

INSERT INTO test_ntile VALUES (1), (2), (3), (4), (5), (6);

SELECT
    id,
    ntile(1) OVER (ORDER BY id ASC) AS a,
    ntile(2) OVER (ORDER BY id ASC) AS b
FROM test_ntile
ORDER BY id ASC;
