-- Verify DPhyp builds synthetic transitive hyperedges only when
-- `enable_join_transitive_predicates` is on (Policy A). Column equivalence classes are
-- also materialized when `query_plan_optimize_join_order_use_proven_uniqueness` is on (they feed
-- canonical proof lookup), but their existence must not enlarge the DPhyp search topology:
-- with the transitive setting off, the selected plan, join clauses, and result must be
-- identical with unique-key costing off and on, and no inner join may appear without a
-- predicate.

SET enable_analyzer = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1; -- pin (randomized in CI): NDV statistics must exist for the transitive dim-dim pair to look attractive
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 0;
SET cross_to_inner_join_rewrite = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_hash_table_stats_for_join_reordering = 0;
SET query_plan_remove_unused_columns = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_algorithm = 'dphyp';

CREATE TABLE ttg_dim_a (key UInt32, name String) ENGINE = MergeTree() PRIMARY KEY key SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ttg_fact  (id UInt32, key UInt32, val Float64) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ttg_dim_b (key UInt32, label String) ENGINE = MergeTree() PRIMARY KEY key SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ttg_dim_a SELECT number + 1, concat('A_', toString(number + 1)) FROM numbers(10);
INSERT INTO ttg_dim_b SELECT number + 1, concat('B_', toString(number + 1)) FROM numbers(10);
INSERT INTO ttg_fact  SELECT number, (number % 10) + 1, number / 100.0 FROM numbers(10000);

-- ==========================================================================
-- 1. Chain A.key = Fact.key AND Fact.key = B.key with the transitive setting
--    off: the dims are connected only transitively, so DPhyp must keep the
--    explicit-edge topology regardless of the unique-key setting.
-- ==========================================================================

SELECT 'case 1: chain, unique keys off - dphyp plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM ttg_dim_a a, ttg_fact f, ttg_dim_b b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS query_plan_optimize_join_order_use_proven_uniqueness = 0
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 1: chain, unique keys on - dphyp plan (must be identical)';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM ttg_dim_a a, ttg_fact f, ttg_dim_b b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS query_plan_optimize_join_order_use_proven_uniqueness = 1
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 1: chain - result check';
SELECT count()
FROM ttg_dim_a a, ttg_fact f, ttg_dim_b b
WHERE a.key = f.key AND f.key = b.key
SETTINGS query_plan_optimize_join_order_use_proven_uniqueness = 1;

-- ==========================================================================
-- 2. The same chain with proven unique keys (`GROUP BY` subqueries): a proof
--    may cap explicit-topology candidates but must not create a synthetic
--    dim-dim DPhyp join while the transitive setting is off.
-- ==========================================================================

SELECT 'case 2: proven keys, unique keys on - dphyp plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT key FROM ttg_dim_a GROUP BY key) a, ttg_fact f, (SELECT key FROM ttg_dim_b GROUP BY key) b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS query_plan_optimize_join_order_use_proven_uniqueness = 1
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 2: proven keys - result check';
SELECT count()
FROM (SELECT key FROM ttg_dim_a GROUP BY key) a, ttg_fact f, (SELECT key FROM ttg_dim_b GROUP BY key) b
WHERE a.key = f.key AND f.key = b.key
SETTINGS query_plan_optimize_join_order_use_proven_uniqueness = 1;

-- ==========================================================================
-- 3. Control: with the independent transitive setting on, DPhyp still builds
--    the synthetic dim-dim join and its predicate is synthesized (no inner
--    join without a clause), independently of the unique-key setting.
-- ==========================================================================

SELECT 'case 3: transitive setting on, unique keys off - dphyp plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM ttg_dim_a a, ttg_fact f, ttg_dim_b b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS enable_join_transitive_predicates = 1, query_plan_optimize_join_order_use_proven_uniqueness = 0
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 3: transitive setting on, unique keys on - dphyp plan (must be identical)';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM ttg_dim_a a, ttg_fact f, ttg_dim_b b
    WHERE a.key = f.key AND f.key = b.key
    SETTINGS enable_join_transitive_predicates = 1, query_plan_optimize_join_order_use_proven_uniqueness = 1
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT 'case 3: transitive setting on - result check';
SELECT count()
FROM ttg_dim_a a, ttg_fact f, ttg_dim_b b
WHERE a.key = f.key AND f.key = b.key
SETTINGS enable_join_transitive_predicates = 1, query_plan_optimize_join_order_use_proven_uniqueness = 1;

DROP TABLE ttg_dim_a;
DROP TABLE ttg_fact;
DROP TABLE ttg_dim_b;
