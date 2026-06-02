DROP TABLE IF EXISTS prewhere_stats_group;
CREATE TABLE prewhere_stats_group (
    a UInt64 STATISTICS(tdigest, countmin),
    b UInt64 STATISTICS(countmin)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO prewhere_stats_group SELECT number, number % 200 FROM numbers(5000) SETTINGS materialize_statistics_on_insert = 1;

ALTER TABLE prewhere_stats_group MATERIALIZE STATISTICS all;

SELECT explain FROM (
    EXPLAIN PLAN actions = 1 SELECT count() FROM prewhere_stats_group WHERE a > 2500 AND b = 1 AND a < 2502
    settings use_statistics = 1
) WHERE explain LIKE '%Prewhere filter column%';

DROP TABLE prewhere_stats_group;
