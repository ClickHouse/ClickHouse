CREATE TABLE test
(
    `key` UInt64,
    `value` Int64
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test SELECT cityHash64(number) AS key, number AS value FROM numbers(100);

SET enable_parallel_replicas = 0;
SET enable_analyzer = 1;
EXPLAIN PLAN
WITH
    view_1 AS
    (
        SELECT
            key,
            ROW_NUMBER() OVER (PARTITION BY key) AS rn
        FROM test
    ),
    view_2 AS
    (
        SELECT
            key,
            count() > 0 AS has_any
        FROM test
        GROUP BY
            key
    ),
    events AS
    (
        SELECT
            *
        FROM view_1 AS v1
        INNER JOIN view_2 AS v2_1 USING (key)
        LEFT JOIN view_2 AS v2_2 USING (key)
        WHERE v1.rn = 1
    )
SELECT count()
FROM events
WHERE v2_1.has_any;
