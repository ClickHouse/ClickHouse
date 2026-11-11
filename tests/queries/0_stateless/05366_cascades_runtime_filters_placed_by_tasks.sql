-- When Cascades distributes the plan, the initiator places no join runtime filters: every task optimizes its
-- fragment again and places the filters of the joins it runs. The initiator's plan therefore has no runtime
-- filter steps, while the tasks still create filters, and the result is the same as without the distributed plan.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 2;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET max_rows_to_group_by = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_rf_fact;
DROP TABLE IF EXISTS t_rf_dim;
CREATE TABLE t_rf_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE t_rf_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rf_fact SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_rf_dim SELECT number, if(number < 10, 'keep', 'skip') FROM numbers(1000);

SELECT '-- no runtime filter steps in the initiator plan';
SELECT count() FROM (EXPLAIN SELECT sum(f.v) FROM t_rf_fact AS f JOIN t_rf_dim AS d ON f.k = d.k WHERE d.name = 'keep')
WHERE explain LIKE '%BuildRuntimeFilter%' OR explain LIKE '%Apply runtime join filter%';

SELECT '-- the tasks create filters, and the result is right';
SELECT sum(f.v) FROM t_rf_fact AS f JOIN t_rf_dim AS d ON f.k = d.k WHERE d.name = 'keep' SETTINGS log_comment = '05366_cascades_query';
SELECT sum(f.v) FROM t_rf_fact AS f JOIN t_rf_dim AS d ON f.k = d.k WHERE d.name = 'keep' SETTINGS make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['RuntimeFiltersCreated']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND current_database = currentDatabase() AND log_comment = '05366_cascades_query')
-- A distributed read of a system table can lose a part that a merge replaces in the meantime.
SETTINGS make_distributed_plan = 0;

DROP TABLE t_rf_fact;
DROP TABLE t_rf_dim;
