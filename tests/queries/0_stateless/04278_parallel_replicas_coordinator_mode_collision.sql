-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106039
--
-- A single query plan that reads the same table in two different parallel-replicas
-- coordination modes (one subquery requires `WithOrder`, the other `Default`) used
-- to throw `Coordination mode mismatch for stream <table>` because the coordinator
-- map was keyed by stream id alone. After the fix the two modes get independent
-- coordinator instances and the query succeeds.

DROP TABLE IF EXISTS t_pr_coord_collision;

CREATE TABLE t_pr_coord_collision (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_pr_coord_collision
SELECT toString(rand() % 100000), number FROM numbers(300000);

INSERT INTO t_pr_coord_collision
SELECT toString(rand() % 100000), number FROM numbers(300000);

-- The sharded-aggregator subquery scatters rows by hash and disables read-in-order,
-- so it announces `Default`. The in-order subquery announces `WithOrder`. Both
-- subqueries share the same `ParallelReplicasReadingCoordinator` instance.
SELECT
    (SELECT count() FROM
        (SELECT a, sum(b) FROM t_pr_coord_collision GROUP BY a
         SETTINGS enable_sharding_aggregator = 0, optimize_aggregation_in_order = 1)) > 0,
    (SELECT count() FROM
        (SELECT a, sum(b) FROM t_pr_coord_collision GROUP BY a
         SETTINGS enable_sharding_aggregator = 1, optimize_aggregation_in_order = 1)) > 0;

DROP TABLE t_pr_coord_collision;
