SET joined_subquery_requires_alias = 0;

DROP TEMPORARY TABLE IF EXISTS test_00744;
CREATE TEMPORARY TABLE test_00744
(
    x Int32
);

INSERT INTO test_00744 VALUES (1);

SELECT x
FROM
(
    SELECT
        x,
        `1`
    FROM
    (
        SELECT x, 1 FROM test_00744
    )
    ALL INNER JOIN
    (
        SELECT
            count(),
            1
        FROM test_00744
    ) jss2 USING (`1`)
    LIMIT 10
);

SELECT
    x,
    `1`
FROM
(
    SELECT x, 1 FROM test_00744
)
ALL INNER JOIN
(
    SELECT
        count(),
        1
    FROM test_00744
) js2 USING (`1`)
LIMIT 10;
