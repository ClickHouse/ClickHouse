-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only.

-- The statistics of a data part are cached per part and column, loaded by the first query that needs
-- them, and merged on the fly for the parts of every query. So a query over the parts left after
-- partition pruning hits the cache as well, a new part is the only miss of the next query, and the
-- statistics are never loaded in the background.

SET enable_analyzer = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_optimize_join_order_limit = 10;
-- The join estimates below must come from the statistics, not from the randomized fuzzing of the join order.
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS fact_05227;
DROP TABLE IF EXISTS dim_05227;

CREATE TABLE fact_05227 (p UInt8, id UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id;
CREATE TABLE dim_05227 (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO fact_05227 SELECT 1, number FROM numbers(100000);
INSERT INTO fact_05227 SELECT 2, number % 10 FROM numbers(1000);
INSERT INTO dim_05227 SELECT number FROM numbers(10000);

-- The first query loads the statistics of the columns it reads (`p` and `id`) of every part.
SELECT count() FROM fact_05227 AS f INNER JOIN dim_05227 AS d ON f.id = d.id WHERE f.p >= 1
SETTINGS log_comment = 'stats_cache_1_cold' FORMAT Null;

-- The same query hits the cache and loads nothing.
SELECT count() FROM fact_05227 AS f INNER JOIN dim_05227 AS d ON f.id = d.id WHERE f.p >= 1
SETTINGS log_comment = 'stats_cache_2_warm' FORMAT Null;

-- A query over the parts left after partition pruning hits the cache as well.
SELECT count() FROM fact_05227 AS f INNER JOIN dim_05227 AS d ON f.id = d.id WHERE f.p = 2
SETTINGS log_comment = 'stats_cache_3_pruned' FORMAT Null;

-- A new part is loaded by the next query; the other parts are still cached.
INSERT INTO fact_05227 SELECT 3, number FROM numbers(1000);

SELECT count() FROM fact_05227 AS f INNER JOIN dim_05227 AS d ON f.id = d.id WHERE f.p >= 1
SETTINGS log_comment = 'stats_cache_4_new_part' FORMAT Null;

-- Dropping the cache makes the next query load everything again.
SYSTEM DROP STATISTICS CACHE;

SELECT count() FROM fact_05227 AS f INNER JOIN dim_05227 AS d ON f.id = d.id WHERE f.p >= 1
SETTINGS log_comment = 'stats_cache_5_dropped' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- A query may build the estimator more than once (for PREWHERE and for the join order), and the
-- second time hits what the first time loaded, so hits alone say nothing about the cache state
-- before the query: report whether the query was served from the cache only.
SELECT
    log_comment,
    ProfileEvents['LoadedStatisticsMicroseconds'] > 0 AS loaded_from_disk,
    ProfileEvents['StatisticsCacheMisses'] > 0 AS misses,
    ProfileEvents['StatisticsCacheHits'] > 0 AND ProfileEvents['StatisticsCacheMisses'] = 0 AS only_hits
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'stats_cache_%'
ORDER BY log_comment;

-- The cached statistics are shared between queries and must not be changed by merging them:
-- the estimated sizes of the joined relations must not drift from one execution to the next.
CREATE TEMPORARY TABLE join_estimates_05227 (run UInt8, line String);

INSERT INTO join_estimates_05227 SELECT 1, explain
FROM (EXPLAIN SELECT count() FROM fact_05227 AS f INNER JOIN dim_05227 AS d ON f.id = d.id WHERE f.id < 5000)
WHERE explain LIKE '%⋈%';

INSERT INTO join_estimates_05227 SELECT 2, explain
FROM (EXPLAIN SELECT count() FROM fact_05227 AS f INNER JOIN dim_05227 AS d ON f.id = d.id WHERE f.id < 5000)
WHERE explain LIKE '%⋈%';

SELECT count() = 2 AS both_runs_estimated, uniqExact(line) = 1 AS same_estimates FROM join_estimates_05227;

DROP TABLE fact_05227;
DROP TABLE dim_05227;
