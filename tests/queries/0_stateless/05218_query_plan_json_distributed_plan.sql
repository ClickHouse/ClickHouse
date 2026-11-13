-- Tags: no-old-analyzer, no-parallel-replicas

-- A query that asks for a distributed plan stores no plan at all.

DROP TABLE IF EXISTS t_05218;

CREATE TABLE t_05218 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05218 SELECT number, number * 2 FROM numbers(1000);

SET log_query_plans = 1;

SELECT sum(v) FROM t_05218 WHERE k % 7 = 0
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
             log_comment = '05218_distributed' FORMAT Null;

-- The ordinary path, which must still capture.
SELECT sum(v) FROM t_05218 WHERE k % 7 = 0
    SETTINGS log_comment = '05218_local' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

-- Each stage of a distributed plan is logged as a query of its own, inheriting the `log_comment`
-- and recording `main` or `stage_<n>_<m>` as its text, so match the user's query by its table.
SELECT
    replaceOne(log_comment, '05218_', '') AS shape,
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes')) AS plan_nodes
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05218_distributed', '05218_local')
    AND position(query, 't_05218') > 0
ORDER BY shape;

DROP TABLE t_05218;
