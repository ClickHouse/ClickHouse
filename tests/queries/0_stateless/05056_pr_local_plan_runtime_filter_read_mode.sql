-- A join runtime filter is pushed into the initiator's local plan but can never reach the replicas:
-- `__applyFilter` is non-deterministic, so `addFilters` drops it from the query shipped to them. The
-- initiator must still announce the same coordination mode as the replicas, otherwise the read fails
-- with "Replica N decided to read in X mode, not in Y".
-- https://github.com/ClickHouse/ClickHouse/issues/95524

DROP TABLE IF EXISTS t_rf_read_mode;
DROP TABLE IF EXISTS b_rf_read_mode;

-- Keep the data tiny: the granules only have to be small enough for the coordinator to hand work to
-- every replica, and each mark is a separate read on shared storage.
CREATE TABLE t_rf_read_mode (a UInt64, v UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;
INSERT INTO t_rf_read_mode SELECT number % 500, number FROM numbers(10000);

CREATE TABLE b_rf_read_mode (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO b_rf_read_mode SELECT number FROM numbers(10);

-- For runs with the old analyzer
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET enable_join_runtime_filters = 1;
-- The runtime filter has to be built whatever the probe side is estimated to be.
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_join_swap_table = false;
-- The aggregation reads in order, which is what picks the coordination mode.
SET optimize_read_in_order = 0;
SET optimize_aggregation_in_order = 1;
-- Pin what decides whether the filter is folded into the read, so the plan below is stable.
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

-- Without this the remote replicas may get no marks at all, and then they never send a read request
-- for the coordinator to check the mode of.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

-- The runtime filter has to be inside the local fragment, below the aggregation that reads in order:
-- that is the plan the initiator announces a coordination mode for.
SELECT replaceAll(replaceRegexpOne(explain, '^[^A-Za-z]*', ''), currentDatabase(), 'default') AS step
FROM (
    EXPLAIN actions = 1
    SELECT sum(x.c)
    FROM (SELECT a, count() AS c FROM t_rf_read_mode GROUP BY a) AS x
    JOIN b_rf_read_mode AS bb ON x.a = bb.a
)
WHERE explain LIKE '%Aggregating%'
   OR explain LIKE '%ReadFromMergeTree%'
   OR explain LIKE '%Read type%'
   OR explain LIKE '%Runtime filters:%'
   OR explain LIKE '%Filter column:%';

SELECT sum(x.c)
FROM (SELECT a, count() AS c FROM t_rf_read_mode GROUP BY a) AS x
JOIN b_rf_read_mode AS bb ON x.a = bb.a;

DROP TABLE t_rf_read_mode;
DROP TABLE b_rf_read_mode;
