CREATE TABLE  table_test
(
    `c1` Int32,
    `c2` Nullable(Int32),
    `c3` Nullable(String),
    `c4` Nullable(Date32),
    `c5` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY c1
SETTINGS index_granularity = 8192;

INSERT INTO table_test SELECT * FROM generateRandom() LIMIT 1000;

SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY c2,
    c4
ORDER BY
    c5 DESC,
    c3 DESC) AS row_n
FROM
    (
    SELECT
        t1.c1 as account_id,
        t1.c2 as c2,
        t1.c3 as c3,
        t1.c4 as c4,
        t1.c5 as c5,
        ROW_NUMBER () OVER (PARTITION BY t1.c2,
        t1.c3,
        t1.c4
    ORDER BY
        t1.c5 DESC) AS rn
    FROM
        table_test AS t1
    GROUP BY
        t1.c1,
        t1.c2,
        t1.c3,
        t1.c4,
        t1.c5
    ORDER BY
        t1.c2,
        t1.c4,
        t1.c5
)
WHERE
    rn = 1
ORDER BY
    c2,
    c4,
    c5,
    c3
FORMAT Null;

DROP TABLE table_test;
