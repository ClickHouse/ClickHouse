-- DPhyp's default topology remains explicit-edge-only, while the dedicated
-- obligation-aware opt-in may add a singleton transitive edge backed by a
-- GROUP BY-proven canonical cap. The selected synthetic join must carry a
-- synthesized equality clause and preserve the query result.

SET enable_analyzer = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 0;
SET cross_to_inner_join_rewrite = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_max_searched_plans = 100000;
SET use_hash_table_stats_for_join_reordering = 0;
SET query_plan_remove_unused_columns = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_algorithm = 'dphyp';
SET query_plan_optimize_join_order_use_proven_uniqueness = 1;

CREATE TABLE dpe_dim_a (key UInt32, name String) ENGINE = MergeTree() PRIMARY KEY key SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dpe_fact  (id UInt32, key UInt32, val Float64) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dpe_dim_b (key UInt32, label String) ENGINE = MergeTree() PRIMARY KEY key SETTINGS auto_statistics_types = 'uniq';

INSERT INTO dpe_dim_a SELECT number + 1, concat('A_', toString(number + 1)) FROM numbers(10);
INSERT INTO dpe_dim_b SELECT number + 1, concat('B_', toString(number + 1)) FROM numbers(10);
INSERT INTO dpe_fact SELECT number, (number % 10) + 1, number / 100.0 FROM numbers(10000);

SELECT 'proven edges off - explicit topology';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT key FROM dpe_dim_a GROUP BY key) a, dpe_fact f, (SELECT key FROM dpe_dim_b GROUP BY key) b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS query_plan_optimize_join_order_dphyp_proven_edges = 0
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'proven edges off - result';
SELECT count()
FROM (SELECT key FROM dpe_dim_a GROUP BY key) a, dpe_fact f, (SELECT key FROM dpe_dim_b GROUP BY key) b
WHERE a.key = f.key AND f.key = b.key
SETTINGS query_plan_optimize_join_order_dphyp_proven_edges = 0;

SELECT 'proven edges on - proven dimension edge';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT key FROM dpe_dim_a GROUP BY key) a, dpe_fact f, (SELECT key FROM dpe_dim_b GROUP BY key) b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS query_plan_optimize_join_order_dphyp_proven_edges = 1
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'proven edges on - result';
SELECT count()
FROM (SELECT key FROM dpe_dim_a GROUP BY key) a, dpe_fact f, (SELECT key FROM dpe_dim_b GROUP BY key) b
WHERE a.key = f.key AND f.key = b.key
SETTINGS query_plan_optimize_join_order_dphyp_proven_edges = 1;

DROP TABLE dpe_dim_a;
DROP TABLE dpe_fact;
DROP TABLE dpe_dim_b;
