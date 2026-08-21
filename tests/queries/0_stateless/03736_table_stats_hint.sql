SET allow_experimental_statistics=1;
SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET enable_join_runtime_filters=0;

CREATE TABLE part
(
    `p_partkey` Int32,
    `p_name` String,
)
ENGINE = MergeTree() ORDER BY p_partkey
SETTINGS auto_statistics_types='tdigest,uniq';

CREATE TABLE partsupp
(
    `ps_partkey` Int32,
    `ps_suppkey` Int32,
    `ps_availqty` Int32,
)
ENGINE = MergeTree() ORDER BY (ps_partkey, ps_suppkey)
SETTINGS auto_statistics_types='tdigest,uniq';


INSERT INTO part SELECT number, number::String FROM numbers(100);
INSERT INTO partsupp SELECT number/2, number%17, number FROM numbers(200);

SET query_plan_join_swap_table = 0;

SELECT '========== with statistics ===========';
SET use_statistics=1;

SELECT explain FROM
(
    EXPLAIN PLAN keep_logical_steps=1, actions=1
    SELECT count() FROM partsupp, part WHERE ps_partkey = p_partkey AND ps_availqty >= 10
)
WHERE explain LIKE '% Join: %' OR explain LIKE '% ResultRows: %' OR explain LIKE '% Expression (%' OR explain LIKE '% ReadFromMergeTree %';


SELECT '========== with 10x hint =============';
SET use_statistics=0;
SET query_plan_optimize_join_order_algorithm='greedy';
-- Statistics hint with all values multiplied by 10 compared to real
SET param__internal_join_table_stat_hints = '
{
    "part": { "cardinality": 1000, "distinct_keys": { "p_partkey" : 1000, "p_name" : 1000 } },
    "partsupp": { "cardinality": 1900, "distinct_keys": { "ps_partkey" : 950, "ps_suppkey" : 170, "ps_availqty" : 200 } }
}';
SET send_logs_level='error'; -- Suppress Warning logs about hint

SELECT explain FROM
(
    EXPLAIN PLAN keep_logical_steps=1, actions=1
    SELECT count() FROM partsupp, part WHERE ps_partkey = p_partkey AND ps_availqty >= 10
)
WHERE explain LIKE '% Join: %' OR explain LIKE '% ResultRows: %' OR explain LIKE '% Expression (%' OR explain LIKE '% ReadFromMergeTree %';

