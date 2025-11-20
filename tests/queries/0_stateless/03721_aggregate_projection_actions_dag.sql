DROP TABLE IF EXISTS t;

CREATE TABLE t (
    id UInt64,
    value UInt64,
    PROJECTION count_by_id
    (
        SELECT
            id,
            count() AS cnt
        GROUP BY id
    )
) ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO t SELECT 1, number * 10 FROM numbers(30);

SET optimize_use_projections = 1;

SELECT
    id,
    count() as cnt
FROM t
GROUP BY id;