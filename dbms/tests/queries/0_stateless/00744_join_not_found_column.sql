CREATE TEMPORARY TABLE test
(
    x Int32
);

INSERT INTO test VALUES (1);

SELECT x
FROM
(
    SELECT
        x,
        1
    FROM test
    ALL INNER JOIN
    (
        SELECT
            count(),
            1
        FROM test
    ) USING (1)
    LIMIT 10
);

SELECT
    x,
    1
FROM test
ALL INNER JOIN
(
    SELECT
        count(),
        1
    FROM test
) USING (1)
LIMIT 10;
