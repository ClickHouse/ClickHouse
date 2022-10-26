DROP TABLE IF EXISTS data_02233;

CREATE TABLE data_02233
(
    `parent_key` Int,
    `child_key` Int,
    `child_key_2` Int,
    `value` Int
)
ENGINE = MergeTree
ORDER BY (parent_key, child_key);

INSERT INTO data_02233 SELECT
    number % 10,
    number % 3,
    intDiv(number, 2),
    number
FROM numbers(100000000);

SELECT *
FROM
(
    SELECT
        parent_key,
        child_key,
        count()
    FROM data_02233
    GROUP BY
        parent_key,
        child_key,
        child_key_2
    SETTINGS optimize_aggregation_in_order = 1
)
EXCEPT
SELECT *
FROM
(
    SELECT
        parent_key,
        child_key,
        count()
    FROM data_02233
    GROUP BY
        parent_key,
        child_key,
        child_key_2
    SETTINGS optimize_aggregation_in_order = 0
);

SELECT *
FROM
(
    SELECT
        parent_key,
        child_key,
        count()
    FROM data_02233
    GROUP BY
        parent_key,
        child_key,
        child_key_2
    SETTINGS optimize_aggregation_in_order = 0
)
EXCEPT
SELECT *
FROM
(
    SELECT
        parent_key,
        child_key,
        count()
    FROM data_02233
    GROUP BY
        parent_key,
        child_key,
        child_key_2
    SETTINGS optimize_aggregation_in_order = 1
);

DROP TABLE data_02233;
