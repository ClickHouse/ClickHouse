DROP TABLE IF EXISTS mv_second;
DROP TABLE IF EXISTS mv_dst;
DROP TABLE IF EXISTS dst;

CREATE TABLE dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE mv_dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE MATERIALIZED VIEW mv_second
TO mv_dst
AS SELECT
    0 AS key,
    value
FROM dst;

DROP TABLE mv_dst;

CREATE MATERIALIZED VIEW mv_dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
AS SELECT
    1 AS key,
    value
FROM dst;

SET use_async_executor_for_materialized_views = 1;

INSERT INTO dst VALUES (1, 'A');

SELECT 'dst', key, value FROM dst ORDER BY ALL;
SELECT 'mv_dst', key, value FROM mv_dst ORDER BY ALL;

DROP TABLE mv_second;
DROP TABLE mv_dst;
DROP TABLE dst;
