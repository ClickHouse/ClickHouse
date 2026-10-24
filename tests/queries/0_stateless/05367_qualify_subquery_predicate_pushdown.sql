-- A predicate pushed into a remote subquery is written into its `HAVING`, which is evaluated before the
-- window functions `QUALIFY` filters by. Moved there, `ts % 20 = 0` would number only the rows it keeps,
-- and `QUALIFY` would then keep a different set. So a subquery with `QUALIFY` takes no pushed predicate.

DROP TABLE IF EXISTS t_qualify_pushdown;
DROP VIEW IF EXISTS v_qualify_pushdown;

CREATE TABLE t_qualify_pushdown (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts);
INSERT INTO t_qualify_pushdown SELECT number % 10, number FROM numbers(10000);

CREATE VIEW v_qualify_pushdown AS
    SELECT tenant, ts FROM t_qualify_pushdown QUALIFY row_number() OVER (PARTITION BY tenant ORDER BY ts) <= 5;

SET allow_push_predicate_ast_for_distributed_subqueries = 1;

SELECT 'local';
SELECT count(), sum(ts) FROM (
    SELECT tenant, ts FROM t_qualify_pushdown QUALIFY row_number() OVER (PARTITION BY tenant ORDER BY ts) <= 5
) WHERE ts % 20 = 0;

SELECT 'remote';
SELECT count(), sum(ts) FROM (
    SELECT tenant, ts FROM remote('127.0.0.2', currentDatabase(), t_qualify_pushdown)
    QUALIFY row_number() OVER (PARTITION BY tenant ORDER BY ts) <= 5
) WHERE ts % 20 = 0;

SELECT 'parallel replicas';
SELECT count(), sum(ts) FROM v_qualify_pushdown WHERE ts % 20 = 0
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_plan_based = 0, serialize_query_plan = 0;

DROP VIEW v_qualify_pushdown;
DROP TABLE t_qualify_pushdown;
