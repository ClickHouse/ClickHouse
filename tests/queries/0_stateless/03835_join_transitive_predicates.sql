-- Tests for transitive predicate inference in the join order optimizer.
--
-- When `enable_join_transitive_predicates` is enabled, the optimizer infers
-- transitive equi-join predicates (e.g., A.x=B.x AND B.x=C.x implies A.x=C.x)
-- so that direct joins between transitively-connected tables are possible.
-- After optimization, `cleanupJoinPredicates` removes redundant predicates
-- and synthesizes missing ones for transitive-only joins.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 1;

-- Dim_A: small dimension (10 rows), unique key
CREATE TABLE Dim_A (
    a_key UInt32,
    a_name String
) ENGINE = MergeTree()
PRIMARY KEY (a_key)
SETTINGS auto_statistics_types = 'uniq';

-- Fact_B: large fact table (10000 rows), FK to Dim_A via b_key
CREATE TABLE Fact_B (
    b_id UInt32,
    b_key UInt32,
    b_value Float64
) ENGINE = MergeTree()
PRIMARY KEY (b_id)
SETTINGS auto_statistics_types = 'uniq';

-- Dim_C: small dimension (10 rows), unique key
CREATE TABLE Dim_C (
    c_key UInt32,
    c_label String
) ENGINE = MergeTree()
PRIMARY KEY (c_key)
SETTINGS auto_statistics_types = 'uniq';

-- Dim_D: small dimension (5 rows), unique key
CREATE TABLE Dim_D (
    d_key UInt32,
    d_code String
) ENGINE = MergeTree()
PRIMARY KEY (d_key)
SETTINGS auto_statistics_types = 'uniq';

-- Populate tables
INSERT INTO Dim_A SELECT number + 1, concat('A_', toString(number + 1)) FROM numbers(10);
INSERT INTO Dim_C SELECT number + 1, concat('C_', toString(number + 1)) FROM numbers(10);
INSERT INTO Dim_D SELECT number + 1, concat('D_', toString(number + 1)) FROM numbers(5);
INSERT INTO Fact_B SELECT number, (number % 10) + 1, number / 100.0 FROM numbers(10000);

-- ==========================================================================
-- Case 1: Basic chain with DPsize (A.key=B.key AND B.key=C.key)
-- Equivalence class: {a_key, b_key, c_key}
-- WITH transitivity: Dim_A and Dim_C join directly via synthesized a_key=c_key.
-- WITHOUT: must route through Fact_B.
-- ==========================================================================

SELECT '-- Case 1: dpsize chain WITH transitivity';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C
    WHERE a_key = b_key AND b_key = c_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT '-- Case 1: dpsize chain WITHOUT transitivity';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C
    WHERE a_key = b_key AND b_key = c_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_join_transitive_predicates = 0
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

-- ==========================================================================
-- Case 2: Basic chain with greedy
-- ==========================================================================

SELECT '-- Case 2: greedy chain WITH transitivity';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C
    WHERE a_key = b_key AND b_key = c_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT '-- Case 2: greedy chain WITHOUT transitivity';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C
    WHERE a_key = b_key AND b_key = c_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 0
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

-- ==========================================================================
-- Case 3: Diamond -- all pairs directly connected
-- (A.key=B.key AND B.key=C.key AND A.key=C.key)
-- One predicate is redundant. Each join step should have exactly one clause.
-- ==========================================================================

SELECT '-- Case 3: diamond';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C
    WHERE a_key = b_key AND b_key = c_key AND a_key = c_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

-- ==========================================================================
-- Case 4: Four-table single equivalence class
-- (A.key=B.key AND B.key=C.key AND C.key=D.key)
-- Single class: {a_key, b_key, c_key, d_key}
-- All dimension tables can join directly; each step has one predicate.
-- ==========================================================================

SELECT '-- Case 4: 4-table chain WITH transitivity';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C, Dim_D
    WHERE a_key = b_key AND b_key = c_key AND c_key = d_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

SELECT '-- Case 4: 4-table chain WITHOUT transitivity';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C, Dim_D
    WHERE a_key = b_key AND b_key = c_key AND c_key = d_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_join_transitive_predicates = 0
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

-- ==========================================================================
-- Case 5: Fan -- two edges to same hub (A.key=B.key AND C.key=B.key)
-- Same equivalence class as chain, different predicate direction.
-- ==========================================================================

SELECT '-- Case 5: fan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM Dim_A, Fact_B, Dim_C
    WHERE a_key = b_key AND c_key = b_key
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

-- ==========================================================================
-- Correctness checks: results must match with and without the setting.
-- ==========================================================================

SELECT '-- Correctness: 3-table chain';
SELECT count() FROM Dim_A, Fact_B, Dim_C WHERE a_key = b_key AND b_key = c_key;
SELECT count() FROM Dim_A, Fact_B, Dim_C WHERE a_key = b_key AND b_key = c_key SETTINGS enable_join_transitive_predicates = 0;

SELECT '-- Correctness: diamond';
SELECT count() FROM Dim_A, Fact_B, Dim_C WHERE a_key = b_key AND b_key = c_key AND a_key = c_key;

SELECT '-- Correctness: 4-table chain';
SELECT count() FROM Dim_A, Fact_B, Dim_C, Dim_D WHERE a_key = b_key AND b_key = c_key AND c_key = d_key;
SELECT count() FROM Dim_A, Fact_B, Dim_C, Dim_D WHERE a_key = b_key AND b_key = c_key AND c_key = d_key SETTINGS enable_join_transitive_predicates = 0;

SELECT '-- Correctness: value check';
SELECT a_name, c_label, count() AS cnt
FROM Dim_A, Fact_B, Dim_C WHERE a_key = b_key AND b_key = c_key
GROUP BY a_name, c_label ORDER BY a_name, c_label LIMIT 5;

SELECT a_name, c_label, count() AS cnt
FROM Dim_A, Fact_B, Dim_C WHERE a_key = b_key AND b_key = c_key
GROUP BY a_name, c_label ORDER BY a_name, c_label LIMIT 5
SETTINGS enable_join_transitive_predicates = 0;

SELECT '-- Correctness: 4-table values';
SELECT a_name, c_label, d_code, count() AS cnt
FROM Dim_A, Fact_B, Dim_C, Dim_D WHERE a_key = b_key AND b_key = c_key AND c_key = d_key
GROUP BY a_name, c_label, d_code ORDER BY a_name LIMIT 5;

SELECT a_name, c_label, d_code, count() AS cnt
FROM Dim_A, Fact_B, Dim_C, Dim_D WHERE a_key = b_key AND b_key = c_key AND c_key = d_key
GROUP BY a_name, c_label, d_code ORDER BY a_name LIMIT 5
SETTINGS enable_join_transitive_predicates = 0;

DROP TABLE Dim_A;
DROP TABLE Fact_B;
DROP TABLE Dim_C;
DROP TABLE Dim_D;
