-- Verify DPhyp exploits transitive predicates correctly.
-- When `enable_join_transitive_predicates` is on, DPhyp adds synthetic hyperedges
-- for transitively-connected relation pairs (via column equivalence classes),
-- enabling direct joins between tables with no explicit predicate.
-- `cleanupJoinPredicates` synthesizes the missing predicates after optimization.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 0; -- pin (randomized in CI): statistics built on INSERT change the join order
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 1;
SET cross_to_inner_join_rewrite = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_hash_table_stats_for_join_reordering = 0;
SET query_plan_remove_unused_columns = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET explain_query_plan_default = 'legacy';

CREATE TABLE tp_dim_a (key UInt32, name String) ENGINE = MergeTree() PRIMARY KEY key SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tp_fact  (id UInt32, key UInt32, val Float64) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tp_dim_b (key UInt32, label String) ENGINE = MergeTree() PRIMARY KEY key SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tp_dim_c (key UInt32, code String) ENGINE = MergeTree() PRIMARY KEY key SETTINGS auto_statistics_types = 'uniq';

INSERT INTO tp_dim_a SELECT number + 1, concat('A_', toString(number + 1)) FROM numbers(10);
INSERT INTO tp_dim_b SELECT number + 1, concat('B_', toString(number + 1)) FROM numbers(10);
INSERT INTO tp_dim_c SELECT number + 1, concat('C_', toString(number + 1)) FROM numbers(5);
INSERT INTO tp_fact  SELECT number, (number % 10) + 1, number / 100.0 FROM numbers(10000);

-- ==========================================================================
-- 1. Chain: A.key = Fact.key AND Fact.key = B.key
--    With transitivity, both DPhyp and DPsize join dims directly (A ⋈ B)
--    via the synthesized predicate, then join with the large Fact.
-- ==========================================================================

SELECT 'case 1: chain - dphyp plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tp_dim_a a, tp_fact f, tp_dim_b b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 1: chain - dpsize plan (for comparison)';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tp_dim_a a, tp_fact f, tp_dim_b b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

-- Verify DPhyp produces correct count
SELECT 'case 1: chain - result check';
SELECT count()
FROM tp_dim_a a, tp_fact f, tp_dim_b b
WHERE a.key = f.key AND f.key = b.key
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

-- ==========================================================================
-- 2. Four-table chain: A.key = Fact.key AND Fact.key = B.key AND B.key = C.key
-- ==========================================================================

SELECT 'case 2: 4-table chain - dphyp plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tp_dim_a a, tp_fact f, tp_dim_b b, tp_dim_c c
    WHERE a.key = f.key AND f.key = b.key AND b.key = c.key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 2: 4-table chain - dpsize plan (for comparison)';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tp_dim_a a, tp_fact f, tp_dim_b b, tp_dim_c c
    WHERE a.key = f.key AND f.key = b.key AND b.key = c.key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 2: 4-table chain - result check';
SELECT count()
FROM tp_dim_a a, tp_fact f, tp_dim_b b, tp_dim_c c
WHERE a.key = f.key AND f.key = b.key AND b.key = c.key
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

-- ==========================================================================
-- 3. Star: Fact joins each dim. Dims connected only transitively.
-- ==========================================================================

SELECT 'case 3: star - dphyp plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tp_fact f, tp_dim_a a, tp_dim_b b, tp_dim_c c
    WHERE f.key = a.key AND f.key = b.key AND f.key = c.key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 3: star - result check';
SELECT count()
FROM tp_fact f, tp_dim_a a, tp_dim_b b, tp_dim_c c
WHERE f.key = a.key AND f.key = b.key AND f.key = c.key
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

-- ==========================================================================
-- 4. Diamond: all pairs directly connected (one predicate is redundant).
--    cleanupJoinPredicates should remove the redundant one.
-- ==========================================================================

SELECT 'case 4: diamond - dphyp plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tp_dim_a a, tp_fact f, tp_dim_b b
    WHERE a.key = f.key AND f.key = b.key AND a.key = b.key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 4: diamond - result check';
SELECT count()
FROM tp_dim_a a, tp_fact f, tp_dim_b b
WHERE a.key = f.key AND f.key = b.key AND a.key = b.key
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

DROP TABLE tp_dim_a;
DROP TABLE tp_fact;
DROP TABLE tp_dim_b;
DROP TABLE tp_dim_c;
