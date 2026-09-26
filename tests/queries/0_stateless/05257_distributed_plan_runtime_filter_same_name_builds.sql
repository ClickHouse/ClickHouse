-- Tags: no-old-analyzer

-- `vs` is joined twice, so one stage holds two `BuildRuntimeFilter` steps for the join inside `vs`.
-- Both steps have the same structural filter name. A deserialized build that is not transported
-- must stay inert. Giving both builds one rendezvous key by that name made the second
-- registration throw a `LOGICAL_ERROR`.

CREATE TABLE r1 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE s (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE x (k UInt64, p UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO r1 SELECT number * 7, 1 FROM numbers(1000);
INSERT INTO s SELECT number, number FROM numbers(5000);
INSERT INTO x SELECT number, number FROM numbers(20000);
CREATE VIEW vs AS SELECT s.v AS v FROM s AS s JOIN r1 AS r ON s.k = r.k;

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, max_rows_to_group_by = 0;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET enable_cascades_optimizer = 1;

SELECT count() FROM x JOIN vs AS a ON x.k = a.v JOIN vs AS b ON x.p = b.v SETTINGS enable_join_runtime_filters = 0;
SELECT count() FROM x JOIN vs AS a ON x.k = a.v JOIN vs AS b ON x.p = b.v SETTINGS distributed_plan_join_runtime_filters = 0;
SELECT count() FROM x JOIN vs AS a ON x.k = a.v JOIN vs AS b ON x.p = b.v SETTINGS distributed_plan_join_runtime_filters = 1;
