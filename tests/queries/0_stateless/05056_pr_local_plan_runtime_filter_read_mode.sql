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

SELECT 'runtime filter conjoined with an ordinary condition';
-- A view's own `ORDER BY` puts the sort inside the fragment, where an equality on the sort key prefix
-- would make the read go in order. The two conditions arrive merged into one `Filter` and both go in
-- - the equality prunes the local read like any other condition - but the fragment derives no ordering
-- from either, so the initiator still reads `Default` alongside the replicas.
DROP TABLE IF EXISTS t2_rf_read_mode;
DROP VIEW IF EXISTS v_rf_read_mode;
CREATE TABLE t2_rf_read_mode (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t2_rf_read_mode SELECT number % 100, number FROM numbers(10000);
CREATE VIEW v_rf_read_mode AS SELECT * FROM t2_rf_read_mode ORDER BY ts;

SET optimize_read_in_order = 1, optimize_aggregation_in_order = 0;
-- The section is about the two conditions arriving as one merged `Filter`, so pin the merging: left
-- apart, the runtime filter reaches the read as its own step instead of the read's own annotation.
SET query_plan_merge_filters = 1;

SELECT replaceAll(replaceRegexpOne(explain, '^[^A-Za-z]*', ''), currentDatabase(), 'default') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT v.ts FROM v_rf_read_mode AS v JOIN b_rf_read_mode AS bb ON v.tenant = bb.a
    WHERE v.tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Runtime filters:%'
   OR explain LIKE '%Filter column%' OR explain LIKE '%Prewhere filter column%';

SELECT v.ts FROM v_rf_read_mode AS v JOIN b_rf_read_mode AS bb ON v.tenant = bb.a
WHERE v.tenant = 5 ORDER BY v.ts LIMIT 5;

DROP VIEW v_rf_read_mode;
DROP TABLE t2_rf_read_mode;
DROP TABLE t_rf_read_mode;
DROP TABLE b_rf_read_mode;
