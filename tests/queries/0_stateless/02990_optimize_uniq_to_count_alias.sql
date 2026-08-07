--https://github.com/ClickHouse/ClickHouse/issues/59999
DROP TABLE IF EXISTS tags;
CREATE TABLE tags (dev_tag String) ENGINE = Memory AS SELECT '1';

SELECT *
FROM
(
    SELECT countDistinct(dev_tag) AS total_devtags
    FROM
    (
        SELECT dev_tag
        FROM
            (
                SELECT *
                FROM tags
            ) AS t
    GROUP BY dev_tag
    ) AS t
) SETTINGS optimize_uniq_to_count=0;

SELECT *
FROM
(
    SELECT countDistinct(dev_tag) AS total_devtags
    FROM
    (
        SELECT dev_tag
        FROM
            (
                SELECT *
                FROM tags
            ) AS t
    GROUP BY dev_tag
    ) AS t
) SETTINGS optimize_uniq_to_count=1;

-- https://github.com/ClickHouse/ClickHouse/issues/62298
DROP TABLE IF EXISTS users;
CREATE TABLE users
(
    `id` Int64,
    `name` String
)
ENGINE = ReplacingMergeTree
ORDER BY (id, name);

INSERT INTO users VALUES (1, 'pufit'), (1, 'pufit2'), (1, 'pufit3');

SELECT uniqExact(id) FROM ( SELECT id FROM users WHERE id = 1 GROUP BY id, name );

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS tags;
