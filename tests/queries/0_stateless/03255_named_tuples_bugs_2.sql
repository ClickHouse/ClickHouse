-- https://github.com/ClickHouse/ClickHouse/issues/70830
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS target;
SET flatten_nested = 0;

CREATE TABLE src
(
    id UInt64,
    minmax Nested(
        min Float64,
        max Float64,
    )
)
ENGINE = MergeTree
ORDER BY (id);

CREATE TABLE target AS src;
INSERT INTO src VALUES (0, [(-10, 10)]);

INSERT INTO target
SELECT
    id,
    groupArray(tuple(bucket_min, bucket_max))
FROM
(
    SELECT
        id,
        minmax.1 AS bucket_min,
        minmax.2 AS bucket_max
    FROM
    (
        select
            id, arrayJoin(minmax) as minmax
        FROM src
    )
)
GROUP BY id;

SELECT * FROM target;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS target;
