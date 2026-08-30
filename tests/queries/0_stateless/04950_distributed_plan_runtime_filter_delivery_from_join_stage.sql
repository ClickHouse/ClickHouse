-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small_build (sid UInt64) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(4000000);
INSERT INTO small_build SELECT number * 1000 FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_cascades_optimizer = 1;
SET max_rows_to_group_by = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1;

-- Simulate a 4 node cluster and lie about the table sizes so Cascades picks a shuffle join: the
-- filter is then built in the join stage while `__applyFilter` sits in the probe scan's PREWHERE,
-- so the receiving stage is one the producing stage depends on - the common runtime filter
-- topology. The wiring admission still uses the real estimates (100 build rows vs ~90K probe
-- rows), and the real build table is tiny, so the filter is ready while the probe scan still
-- runs.
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"work_weight":1,"exchange_fixed_overhead":3000,"network_weight":1,"sequential_weight":32}';
SET param__internal_join_table_stat_hints = '{
    "big":         { "cardinality": 500000000, "avg_row_bytes": 16, "distinct_keys": { "bid": 500000000, "v": 400000000 } },
    "small_build": { "cardinality": 400000000, "avg_row_bytes": 16, "distinct_keys": { "sid": 400000000 } }
}';

SELECT count() FROM big, small_build AS s WHERE bid = s.sid AND v < 90000
SETTINGS log_comment = '04950_delivery_from_join_stage';

SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log, text_log;

-- The filter must reach the probe-scan tasks (stage_0) and be registered there. Registration is
-- logged by the receive branch when the merged filter arrives, so this holds even if the filter
-- arrives near the end of the scan. When the delivery to the scan stage is not wired, the only
-- registrations happen in the join-stage tasks and the scan tasks log nothing.
SELECT count() > 0 FROM system.text_log
WHERE logger_name = 'RuntimeFilter' AND message LIKE 'Registered runtime filter%' AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE type = 'QueryFinish' AND query LIKE 'stage_0_%' AND log_comment = '04950_delivery_from_join_stage'
        AND event_date >= yesterday())
    AND event_date >= yesterday();
