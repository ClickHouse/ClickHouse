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

DROP TABLE IF EXISTS tags;
