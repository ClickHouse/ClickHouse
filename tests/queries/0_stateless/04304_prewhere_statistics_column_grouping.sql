-- Tags: no-fasttest
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject 0, which would skip grouping/reordering and produce no PREWHERE output

DROP TABLE IF EXISTS prewhere_stats_group;
CREATE TABLE prewhere_stats_group (
    a UInt64 STATISTICS(tdigest, countmin),
    b UInt64 STATISTICS(countmin)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO prewhere_stats_group SELECT number, number % 200 FROM numbers(5000) SETTINGS materialize_statistics_on_insert = 1;

ALTER TABLE prewhere_stats_group MATERIALIZE STATISTICS all;

SET use_statistics = 1;

SELECT explain FROM (
    EXPLAIN PLAN actions = 1 SELECT count() FROM prewhere_stats_group WHERE a > 2500 AND b = 1 AND a < 2502
) WHERE explain LIKE '%Prewhere filter column%';

DROP TABLE prewhere_stats_group;
