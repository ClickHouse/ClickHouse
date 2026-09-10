-- Tags: zookeeper
SET session_timezone = 'UTC';

SELECT '-- Original issue with max_insert_delayed_streams_for_parallel_write <= 1';
/*

    This is the expected behavior when mv deduplication is set to false.

    - 1st insert works for landing and mv tables
    - 2nd insert gets first block 20220901 deduplicated and second one inserted in landing table
    - 2nd insert gets both blocks inserted in mv table

*/
SET deduplicate_blocks_in_dependent_materialized_views = 0, max_insert_delayed_streams_for_parallel_write = 0;

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

SELECT '-- Original issue with deduplicate_blocks_in_dependent_materialized_views = 0 AND max_insert_delayed_streams_for_parallel_write > 1';
/*

    This is the unexpected behavior due to setting max_insert_delayed_streams_for_parallel_write > 1.

    This unexpected behavior was present since version 21.9 or earlier but due to this PR https://github.com/ClickHouse/ClickHouse/pull/34780
    when max_insert_delayed_streams_for_parallel_write gets disabled by default the issue was mitigated.

    This is what happens:

    - 1st insert works for landing and mv tables
    - 2nd insert gets first block 20220901 deduplicated and second one inserted in landing table
    - 2nd insert is not inserting anything in mv table due to a bug computing blocks to be discarded, now that block is inserted because deduplicate_blocks_in_dependent_materialized_views=0

    Now it is fixed.
*/
SET deduplicate_blocks_in_dependent_materialized_views = 0, max_insert_delayed_streams_for_parallel_write = 1000;

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

SELECT '-- Original issue with deduplicate_blocks_in_dependent_materialized_views = 1 AND max_insert_delayed_streams_for_parallel_write > 1';
/*

    By setting deduplicate_blocks_in_dependent_materialized_views = 1 we can make the code go through a different path getting an expected
    behavior again, even with max_insert_delayed_streams_for_parallel_write > 1.

    This is what happens now:

    - 1st insert works for landing and mv tables
    - 2nd insert gets first block 20220901 deduplicated for landing and both rows are inserted for mv tables

*/
SET deduplicate_blocks_in_dependent_materialized_views = 1, max_insert_delayed_streams_for_parallel_write = 1000;

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

SELECT '-- Regression introduced in https://github.com/ClickHouse/ClickHouse/pull/54184';
/*

    This is a test to prevent regression introduced in https://github.com/ClickHouse/ClickHouse/pull/54184 from happening again.

    The PR was trying to fix the unexpected behavior when deduplicate_blocks_in_dependent_materialized_views = 0 AND
    max_insert_delayed_streams_for_parallel_write > 1 but it ended up adding a new regression.

*/
SET deduplicate_blocks_in_dependent_materialized_views = 0, max_insert_delayed_streams_for_parallel_write = 0;

CREATE TABLE landing
(
    `time` DateTime,
    `pk1` LowCardinality(String),
    `pk2` LowCardinality(String),
    `pk3` LowCardinality(String),
    `pk4` String
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/' || currentDatabase() || '/landing/{shard}/', '{replica}')
ORDER BY (pk1, pk2, pk3, pk4);

CREATE TABLE ds
(
    `pk1` LowCardinality(String),
    `pk2` LowCardinality(String),
    `pk3` LowCardinality(String),
    `pk4` LowCardinality(String),
    `occurences` AggregateFunction(count)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/' || currentDatabase() || '/ds/{shard}/', '{replica}')
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
