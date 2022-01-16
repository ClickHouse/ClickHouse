DROP TABLE IF EXISTS normal;

CREATE TABLE normal
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32,
    PROJECTION aaaa
    (
        SELECT
            ts,
            key,
            value
        ORDER BY (ts, key)
    )
)
ENGINE = MergeTree
ORDER BY (key, ts);

INSERT INTO normal SELECT
    1,
    now() + number,
    number
FROM numbers(100000);

SET allow_experimental_projection_optimization=1, optimize_aggregation_in_order=1, force_optimize_projection = 1;

SELECT toUnixTimestamp(toStartOfHour(ts) AS a) FROM normal WHERE ts > '2021-12-06 22:00:00' GROUP BY a LIMIT 5;

DROP TABLE IF EXISTS agg;

CREATE TABLE agg
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32,
    PROJECTION aaaa
    (
        SELECT
            ts,
            key,
            sum(value)
        GROUP BY (ts, key)
    )
)
ENGINE = MergeTree
ORDER BY (key, ts);

INSERT INTO agg SELECT
    1,
    now() + number,
    number
FROM numbers(100000);

SET allow_experimental_projection_optimization=1, optimize_aggregation_in_order=1, force_optimize_projection = 1;

SELECT toUnixTimestamp(toStartOfHour(ts) AS a) FROM agg WHERE ts > '2021-12-06 22:00:00' GROUP BY a LIMIT 5;
