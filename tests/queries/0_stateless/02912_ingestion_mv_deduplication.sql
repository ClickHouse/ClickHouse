-- Tags: replica
SET session_timezone = 'UTC';
SET deduplicate_blocks_in_dependent_materialized_views = 0;

SELECT '-- Original issue with max_insert_delayed_streams_for_parallel_write = 1';
/*

    This is the expected behavior when mv deduplication is set to false. TODO: Add more details about what happened.

*/
SET max_insert_delayed_streams_for_parallel_write = 1;

CREATE TABLE landing
(
    time DateTime,
    number Int64
)
Engine=ReplicatedReplacingMergeTree('/clickhouse/' || currentDatabase() || '/landing/{shard}/', '{replica}')
PARTITION BY toYYYYMMDD(time)
ORDER BY time;

CREATE MATERIALIZED VIEW mv
ENGINE = ReplicatedSummingMergeTree('/clickhouse/' || currentDatabase() || '/mv/{shard}/', '{replica}')
PARTITION BY toYYYYMMDD(hour) ORDER BY hour
AS SELECT
    toStartOfHour(time) AS hour,
    sum(number) AS sum_amount
FROM landing
GROUP BY hour;

INSERT INTO landing VALUES ('2022-09-01 12:23:34', 42);
INSERT INTO landing VALUES ('2022-09-01 12:23:34', 42),('2023-09-01 12:23:34', 42);

SELECT '-- Landing';
SELECT * FROM landing FINAL ORDER BY time;
SELECT '-- MV';
SELECT * FROM mv FINAL ORDER BY hour;

DROP TABLE IF EXISTS landing SYNC;
DROP TABLE IF EXISTS mv SYNC;

SELECT '-- Original issue with max_insert_delayed_streams_for_parallel_write > 1';
/*

    This is the unexpected behavior. TODO: Add more details about what happened.

*/
SET max_insert_delayed_streams_for_parallel_write = 10;

CREATE TABLE landing
(
    time DateTime,
    number Int64
)
Engine=ReplicatedReplacingMergeTree('/clickhouse/' || currentDatabase() || '/landing/{shard}/', '{replica}')
PARTITION BY toYYYYMMDD(time)
ORDER BY time;

CREATE MATERIALIZED VIEW mv
ENGINE = ReplicatedSummingMergeTree('/clickhouse/' || currentDatabase() || '/mv/{shard}/', '{replica}')
PARTITION BY toYYYYMMDD(hour) ORDER BY hour
AS SELECT
    toStartOfHour(time) AS hour,
    sum(number) AS sum_amount
FROM landing
GROUP BY hour;

INSERT INTO landing VALUES ('2022-09-01 12:23:34', 42);
INSERT INTO landing VALUES ('2022-09-01 12:23:34', 42),('2023-09-01 12:23:34', 42);

SELECT '-- Landing';
SELECT * FROM landing FINAL ORDER BY time;
SELECT '-- MV';
SELECT * FROM mv FINAL ORDER BY hour;

SET max_insert_delayed_streams_for_parallel_write = 1;
DROP TABLE IF EXISTS landing SYNC;
DROP TABLE IF EXISTS mv SYNC;

SELECT '-- Regression introduced in https://github.com/ClickHouse/ClickHouse/pull/54184';
/*

    This is a test to prevent regression introduced in https://github.com/ClickHouse/ClickHouse/pull/54184 from happening again

*/

CREATE TABLE landing
(
    `time` DateTime,
    `pk1` LowCardinality(String),
    `pk2` LowCardinality(String),
    `pk3` LowCardinality(String),
    `pk4` String
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/landing', '{replica}')
ORDER BY (pk1, pk2, pk3, pk4);

CREATE TABLE ds
(
    `pk1` LowCardinality(String),
    `pk2` LowCardinality(String),
    `pk3` LowCardinality(String),
    `pk4` LowCardinality(String),
    `occurences` AggregateFunction(count)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{layer}-{shard}/ds', '{replica}')
ORDER BY (pk1, pk2, pk3, pk4);

CREATE MATERIALIZED VIEW mv TO ds AS
SELECT
    pk1,
    pk2,
    pk4,
    pk3,
    countState() AS occurences
FROM landing
GROUP BY pk1, pk2, pk4, pk3;

INSERT INTO landing (time, pk1, pk2, pk4, pk3)
VALUES ('2023-01-01 00:00:00','org-1','prod','login','user'),('2023-01-01 00:00:00','org-1','prod','login','user'),('2023-01-01 00:00:00','org-1','prod','login','user'),('2023-02-01 00:00:00','org-1','stage','login','user'),('2023-02-01 00:00:00','org-1','prod','login','account'),('2023-02-01 00:00:00','org-1','prod','checkout','user'),('2023-03-01 00:00:00','org-1','prod','login','account'),('2023-03-01 00:00:00','org-1','prod','login','account');

SELECT '-- Landing (Agg/Replacing)MergeTree';
SELECT
    pk1,
    pk2,
    pk4,
    pk3,
    count() as occurences
FROM landing
GROUP BY pk1, pk2, pk4, pk3
ORDER BY pk1, pk2, pk4, pk3;

SELECT '--- MV';
SELECT
    pk1,
    pk2,
    pk4,
    pk3,
    countMerge(occurences) AS occurences
FROM ds
GROUP BY pk1, pk2, pk4, pk3
ORDER BY pk1, pk2, pk4, pk3;

DROP TABLE IF EXISTS landing SYNC;
DROP TABLE IF EXISTS ds SYNC;
DROP TABLE IF EXISTS mv SYNC;
