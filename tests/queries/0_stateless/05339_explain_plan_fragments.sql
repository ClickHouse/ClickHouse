-- `EXPLAIN PLAN fragments = 1`: the plan of a query distributed to stateless workers, printed per query fragment.
-- The queries are only explained, never executed.

DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS big;

CREATE TABLE small(sid UInt64, s Array(Int64)) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE big(bid UInt64, b Array(Int64)) ENGINE = MergeTree ORDER BY bid;

INSERT INTO small SELECT number, [number] FROM numbers(0, 1000);
INSERT INTO big SELECT number, [number] FROM numbers(0, 100000);

SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 0;
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- The test harness randomizes these, and aggregation in order changes the plan.
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
-- The test harness randomizes these too: a random join order or probe-row threshold changes or drops the runtime filters.
SET join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
-- In ClickHouse Cloud a scan on shared storage is replaced by `ReadFromMergeTreeAtWorker`, which prints no table name.
SET distributed_plan_prefer_replicas_over_workers = 1;

SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    use_statistics = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';

SELECT '-- shuffle join with aggregation';
EXPLAIN fragments = 1
SELECT small.sid, count()
FROM big, small
WHERE small.sid = (big.bid + 1) % 5000
GROUP BY small.sid
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- broadcast join';
EXPLAIN fragments = 1
SELECT count()
FROM big, small
WHERE small.sid = (big.bid + 1) % 5000
SETTINGS distributed_plan_max_rows_to_broadcast = 1000000;

SELECT '-- aggregation';
EXPLAIN fragments = 1
SELECT bid % 10 AS k, sum(bid)
FROM big
GROUP BY k;

SELECT '-- order by';
EXPLAIN fragments = 1
SELECT bid
FROM big
ORDER BY bid DESC;

-- The `Output` line of a fragment shows what its send step sends.
SELECT '-- pretty';
EXPLAIN fragments = 1, pretty = 1
SELECT bid % 10 AS k, sum(bid)
FROM big
GROUP BY k;

SELECT '-- make_distributed_plan = 0';
EXPLAIN fragments = 1
SELECT bid % 10 AS k, sum(bid)
FROM big
GROUP BY k
SETTINGS make_distributed_plan = 0;
SELECT '-- make_distributed_plan = 0, without fragments';
EXPLAIN
SELECT bid % 10 AS k, sum(bid)
FROM big
GROUP BY k
SETTINGS make_distributed_plan = 0;

SELECT '-- fallback to local execution';
EXPLAIN fragments = 1
SELECT bid % 10 AS k, sum(bid)
FROM big
GROUP BY k WITH TOTALS
SETTINGS distributed_plan_fallback_to_local_execution = 1;

SELECT '-- errors';
EXPLAIN fragments = 1, json = 1 SELECT count() FROM big; -- { serverError NOT_IMPLEMENTED }
EXPLAIN fragments = 1, distributed = 1 SELECT count() FROM big; -- { serverError NOT_IMPLEMENTED }
EXPLAIN fragments = 1, optimize = 0 SELECT count() FROM big; -- { serverError BAD_ARGUMENTS }

DROP TABLE small;
DROP TABLE big;
