DROP TABLE IF EXISTS combinator_argMin_table_r1 SYNC;
DROP TABLE IF EXISTS combinator_argMin_table_r2 SYNC;

CREATE TABLE combinator_argMin_table_r1
(
    `id` Int32,
    `value` Int32,
    `agg_time` DateTime,
    PROJECTION first_items
    (
        SELECT
            id,
            minArgMin(agg_time, value),
            maxArgMax(agg_time, value)
        GROUP BY id
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03128/combinator_argMin_table', 'r1')
ORDER BY (id);

INSERT INTO combinator_argMin_table_r1
    SELECT
        number % 10 as id,
        number as value,
        '2024-01-01 00:00:00' + INTERVAL number SECOND
    FROM
        numbers(100);

INSERT INTO combinator_argMin_table_r1
    SELECT
        number % 10 as id,
        number * 10 as value,
        '2024-01-01 00:00:00' + INTERVAL number SECOND
    FROM
        numbers(100);

SELECT
    id,
    minArgMin(agg_time, value),
    maxArgMax(agg_time, value)
FROM combinator_argMin_table_r1
GROUP BY id
ORDER BY id
SETTINGS force_optimize_projection=1;

-- We check replication by creating another replica
CREATE TABLE combinator_argMin_table_r2
(
    `id` Int32,
    `value` Int32,
    `agg_time` DateTime,
    PROJECTION first_items
        (
        SELECT
            id,
            minArgMin(agg_time, value),
            maxArgMax(agg_time, value)
        GROUP BY id
        )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03128/combinator_argMin_table', 'r2')
ORDER BY (id);

SYSTEM SYNC REPLICA combinator_argMin_table_r2;

SELECT
    id,
    minArgMin(agg_time, value),
    maxArgMax(agg_time, value)
FROM combinator_argMin_table_r2
GROUP BY id
ORDER BY id
SETTINGS force_optimize_projection=1;
