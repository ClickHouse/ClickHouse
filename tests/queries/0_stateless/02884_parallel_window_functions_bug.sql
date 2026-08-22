CREATE TABLE IF NOT EXISTS posts
(
    `page_id` LowCardinality(String),
    `post_id` String CODEC(LZ4),
    `host_id` UInt32 CODEC(T64, LZ4),
    `path_id` UInt32,
    `created` DateTime CODEC(T64, LZ4),
    `as_of` DateTime CODEC(T64, LZ4)
)
ENGINE = ReplacingMergeTree(as_of)
PARTITION BY toStartOfMonth(created)
ORDER BY (page_id, post_id);

CREATE TABLE IF NOT EXISTS post_metrics
(
    `page_id` LowCardinality(String),
    `post_id` String CODEC(LZ4),
    `created` DateTime CODEC(T64, LZ4),
    `impressions` UInt32 CODEC(T64, LZ4),
    `clicks` UInt32 CODEC(T64, LZ4),
    `as_of` DateTime CODEC(T64, LZ4)
)
ENGINE = ReplacingMergeTree(as_of)
PARTITION BY toStartOfMonth(created)
ORDER BY (page_id, post_id);

INSERT INTO posts SELECT
    repeat('a', (number % 10) + 1),
    toString(number),
    number % 10,
    number,
    now() - toIntervalMinute(number),
    now()
FROM numbers(100000);

INSERT INTO post_metrics SELECT
    repeat('a', (number % 10) + 1),
    toString(number),
    now() - toIntervalMinute(number),
    number * 100,
    number * 10,
    now()
FROM numbers(100000);

SELECT
    host_id,
    path_id,
    max(rank) AS rank
FROM
(
    WITH
        as_of_posts AS
        (
            SELECT
                *,
                row_number() OVER (PARTITION BY (page_id, post_id) ORDER BY as_of DESC) AS row_num
            FROM posts
            WHERE (created >= subtractHours(now(), 24)) AND (host_id > 0)
        ),
        as_of_post_metrics AS
        (
            SELECT
                *,
                row_number() OVER (PARTITION BY (page_id, post_id) ORDER BY as_of DESC) AS row_num
            FROM post_metrics
            WHERE created >= subtractHours(now(), 24)
        )
    SELECT
        page_id,
        post_id,
        host_id,
        path_id,
        impressions,
        clicks,
        ntile(20) OVER (PARTITION BY page_id ORDER BY clicks ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rank
    FROM as_of_posts
    GLOBAL LEFT JOIN as_of_post_metrics USING (page_id, post_id, row_num)
    WHERE (row_num = 1) AND (impressions > 0)
) AS t
WHERE t.rank > 18
GROUP BY
    host_id,
    path_id
FORMAT Null;
