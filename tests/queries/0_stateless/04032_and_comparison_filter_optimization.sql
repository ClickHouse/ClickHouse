SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;
SET optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS 04032_t;

CREATE TABLE 04032_t
(
    i Int32,
    u UInt8,
    f Float64,
    s String,
    lc LowCardinality(String),
    dt DateTime('UTC')
)
ENGINE = Memory;

INSERT INTO 04032_t VALUES (1, 10, 1.5, 'a', 'x', '2024-01-01 00:00:00'), (3, 30, 3.0, 'c', 'y', '2024-06-15 12:00:00'), (5, 50, 5.5, 'e', 'z', '2025-01-01 00:00:00');

-- =====================================================================
-- Section 1: equals vs equals — redundancy and conflict
-- =====================================================================

-- same value, same type → redundant, WHERE = single equals
SELECT 'eq_eq_same';
SELECT * FROM 04032_t WHERE i = 3 AND i = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- different value → conflict → WHERE = CONSTANT 0
SELECT 'eq_eq_diff';
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- constant on left side → conflict → CONSTANT 0
SELECT 'eq_eq_flip';
SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- cross-type: Int32 column vs UInt8 constant, same value → redundant
SELECT 'eq_eq_cross_same';
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) SETTINGS optimize_and_compare_chain_pruning = 1;

-- cross-type: Int32 column vs UInt8 constant, different value → conflict
SELECT 'eq_eq_cross_diff';
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_and_compare_chain_pruning = 1;

-- cross-type: Int32 column vs Float64 constant, lossless → redundant
SELECT 'eq_eq_float_same';
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- cross-type: Int32 column vs Float64 constant, lossy → i = 3.5 is always false for Int32 → CONFLICT
SELECT 'eq_eq_float_lossy';
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 2: equals vs less/greater — prune or conflict
-- =====================================================================

-- a = 3 AND a < 5 → prune a < 5, WHERE = single equals
SELECT 'eq_lt_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a < 2 → conflict → CONSTANT 0
SELECT 'eq_lt_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a > 1 → prune a > 1, WHERE = single equals
SELECT 'eq_gt_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a > 5 → conflict → CONSTANT 0
SELECT 'eq_gt_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a <= 3 → prune a <= 3
SELECT 'eq_le_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a >= 3 → prune a >= 3
SELECT 'eq_ge_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a <= 2 → conflict → CONSTANT 0
SELECT 'eq_le_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a >= 4 → conflict → CONSTANT 0
SELECT 'eq_ge_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_and_compare_chain_pruning = 1;

-- cross-type: equals Int32(3) AND less Float64(3.5) → float rewrite: i < 3.5 → i <= 3, then equals prunes it
SELECT 'eq_lt_cross_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- cross-type: equals Int32(3) AND less Float64(2.5) → float rewrite: i < 2.5 → i <= 2, conflict with i = 3
SELECT 'eq_lt_cross_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 3: equals vs notEquals
-- =====================================================================

-- a = 3 AND a != 5 → prune a != 5, WHERE = single equals
SELECT 'eq_ne_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i != 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i != 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a != 3 → conflict → CONSTANT 0
SELECT 'eq_ne_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 4: notEquals vs range — prune notEquals when implied
-- =====================================================================

-- a != 3 AND a < 3 → prune a != 3 (a < 3 implies a != 3)
SELECT 'ne_lt_prune';
SELECT * FROM 04032_t WHERE i != 3 AND i < 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i < 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i < 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i < 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a != 3 AND a > 3 → prune a != 3
SELECT 'ne_gt_prune';
SELECT * FROM 04032_t WHERE i != 3 AND i > 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i > 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a != 3 AND a <= 3 → keep both (a could be 2)
SELECT 'ne_le_keep';
SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a != 3 AND a >= 3 → keep both (a could be 4)
SELECT 'ne_ge_keep';
SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a != 3 AND a != 3 → redundant notEquals
SELECT 'ne_ne_same';
SELECT * FROM 04032_t WHERE i != 3 AND i != 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i != 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a != 3 AND a != 5 → keep both (independent)
SELECT 'ne_ne_diff';
SELECT * FROM 04032_t WHERE i != 3 AND i != 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i != 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 5: less vs less — tighter bound wins
-- =====================================================================

-- a < 5 AND a < 3 → keep a < 3 only
SELECT 'lt_lt_tighter';
SELECT * FROM 04032_t WHERE i < 5 AND i < 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 5 AND i < 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i < 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i < 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a <= 5 AND a <= 3 → keep a <= 3 only
SELECT 'le_le_tighter';
SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a < 3 AND a <= 3 → keep a < 3 (stricter)
SELECT 'lt_le_same_val';
SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a <= 3 AND a < 3 → keep a < 3 (stricter)
SELECT 'le_lt_same_val';
SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 6: greater vs greater — tighter bound wins
-- =====================================================================

-- a > 1 AND a > 3 → keep a > 3 only
SELECT 'gt_gt_tighter';
SELECT * FROM 04032_t WHERE i > 1 AND i > 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i > 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a >= 1 AND a >= 3 → keep a >= 3 only
SELECT 'ge_ge_tighter';
SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a > 3 AND a >= 3 → keep a > 3 (stricter)
SELECT 'gt_ge_same_val';
SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 7: less vs greater — range validity
-- =====================================================================

-- a < 5 AND a > 1 → valid range, keep both
SELECT 'lt_gt_valid';
SELECT * FROM 04032_t WHERE i < 5 AND i > 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 5 AND i > 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i > 1 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i > 1 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a < 1 AND a > 5 → conflict (empty range) → CONSTANT 0
SELECT 'lt_gt_conflict';
SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a <= 3 AND a >= 3 → valid (point), keep both
SELECT 'le_ge_point';
SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a < 3 AND a >= 3 → conflict → CONSTANT 0
SELECT 'lt_ge_conflict';
SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a <= 3 AND a > 3 → conflict → CONSTANT 0
SELECT 'le_gt_conflict';
SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a < 3 AND a > 3 → conflict → CONSTANT 0
SELECT 'lt_gt_conflict_same';
SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- cross-type: less Float vs greater Int, valid range
SELECT 'lt_gt_cross';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 8: constant on left (flip normalization)
-- =====================================================================

SELECT 'flip_less';
SELECT * FROM 04032_t WHERE 5 > i AND i > 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE 5 > i AND i > 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND i > 1 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND i > 1 SETTINGS optimize_and_compare_chain_pruning = 1;

SELECT 'flip_equals_and_range';
SELECT * FROM 04032_t WHERE 3 = i AND 10 > i ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE 3 = i AND 10 > i ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND 10 > i SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND 10 > i SETTINGS optimize_and_compare_chain_pruning = 1;

SELECT 'flip_both_sides';
SELECT * FROM 04032_t WHERE 5 > i AND 1 < i ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE 5 > i AND 1 < i ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND 1 < i SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND 1 < i SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 9: multiple expressions — independent optimization
-- =====================================================================

SELECT 'multi_expr';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 SETTINGS optimize_and_compare_chain_pruning = 1;

SELECT 'multi_expr_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 10: three+ filters on same expression
-- =====================================================================

-- a = 3 AND a > 1 AND a < 5 → prune range, keep a = 3 only
SELECT 'eq_range_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a > 1 AND a < 5 AND a > 3 → tighten to a > 3 AND a < 5
SELECT 'range_tighten';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a = 3 AND a < 5 AND a > 5 → conflict → CONSTANT 0
SELECT 'three_way_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 11: LowCardinality and String columns
-- =====================================================================

SELECT 'lc_eq_eq';
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' SETTINGS optimize_and_compare_chain_pruning = 1;

SELECT 'lc_eq_conflict';
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_and_compare_chain_pruning = 1;

SELECT 'str_eq_eq';
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' SETTINGS optimize_and_compare_chain_pruning = 1;

SELECT 'str_eq_conflict';
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 12: transitive inference + conflict detection (commit d4c0509a0c3)
-- =====================================================================

-- x = 3 AND x = y AND y = 5 → transitive infers x=5, then conflict x=3 vs x=5 → FALSE
SELECT 'transitive_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- x < 3 AND y > 3 AND y < 10 → should NOT produce redundant x < 10
SELECT 'transitive_no_redundant';
SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 SETTINGS optimize_and_compare_chain_pruning = 1;

-- a > b AND b > 3 → infers a > 3
SELECT 'transitive_infer';
SELECT * FROM 04032_t WHERE i > u AND u > 2 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > u AND u > 2 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND u > 2 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND u > 2 SETTINGS optimize_and_compare_chain_pruning = 1;

-- chain: a = b AND b = 3 → infers a = 3
SELECT 'transitive_eq_chain';
SELECT * FROM 04032_t WHERE i = u AND u = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = u AND u = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = u AND u = 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = u AND u = 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 13: non-comparison operands preserved
-- =====================================================================

SELECT 'mixed_operands';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 14: notEquals chain → NOT IN conversion
-- =====================================================================

SET optimize_min_inequality_conjunction_chain_length = 2;

SELECT 'ne_chain_not_in';
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 SETTINGS optimize_and_compare_chain_pruning = 1;

-- notEquals chain with redundant duplicate
SELECT 'ne_chain_dedup';
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 15: edge cases
-- =====================================================================

-- single comparison (no optimization needed)
SELECT 'single';
SELECT * FROM 04032_t WHERE i = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- non-constant expression (not optimizable), WHERE = AND preserved
SELECT 'non_const';
SELECT * FROM 04032_t WHERE i > u AND i < u ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > u AND i < u ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND i < u SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND i < u SETTINGS optimize_and_compare_chain_pruning = 1;

-- NULL handling: comparison with NULL should not crash
SELECT 'null_safe';
SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 16: Mixed types — string constants vs Int32 column
-- =====================================================================

-- String '3' parseable as Int32, same value as 3 → redundant
SELECT 'int_str_eq_same';
SELECT * FROM 04032_t WHERE i = '3' AND i = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = '3' AND i = 3 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = 3 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = 3 SETTINGS optimize_and_compare_chain_pruning = 1;

-- String constants '3' vs '5' on Int32 column → conflict
SELECT 'int_str_eq_diff';
SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_and_compare_chain_pruning = 1;

-- String range on Int32 column
SELECT 'int_str_range';
SELECT * FROM 04032_t WHERE i > '1' AND i < '5' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > '1' AND i < '5' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > '1' AND i < '5' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > '1' AND i < '5' SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 17: Mixed types — integer constants vs Float64 column
-- =====================================================================

-- Int 3 converts losslessly to Float64(3.0), same as 3.0 → redundant
SELECT 'float_int_eq_same';
SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- Int 3 vs Int 4 on Float64 column → conflict
SELECT 'float_int_eq_diff';
SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_and_compare_chain_pruning = 1;

-- Int range on Float64 column
SELECT 'float_int_range';
SELECT * FROM 04032_t WHERE f > 1 AND f < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f > 1 AND f < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > 1 AND f < 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > 1 AND f < 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- Int equals and Float less on Float64 column → prune
SELECT 'float_int_eq_float_lt';
SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 18: Mixed types — string constants vs Float64 column
-- =====================================================================

-- String '1.5' parses to Float64(1.5), same value → redundant
SELECT 'float_str_eq_same';
SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- String '3.0' and int less on Float64 column → prune
SELECT 'float_str_and_int';
SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- String '3.0' vs String '5.0' on Float64 column → conflict
SELECT 'float_str_eq_diff';
SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_and_compare_chain_pruning = 1;

-- String range on Float64 column
SELECT 'float_str_range';
SELECT * FROM 04032_t WHERE f > '1' AND f < '5' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f > '1' AND f < '5' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > '1' AND f < '5' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > '1' AND f < '5' SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 19: DateTime column with string constants
-- =====================================================================

-- Same DateTime string → redundant
SELECT 'dt_str_eq_same';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' SETTINGS optimize_and_compare_chain_pruning = 1;

-- Different DateTime strings → conflict
SELECT 'dt_str_eq_diff';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 1;

-- DateTime string range — valid
SELECT 'dt_str_range';
SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 1;

-- DateTime equals + greater conflict
SELECT 'dt_str_eq_gt_conflict';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 1;

-- DateTime equals + less prune
SELECT 'dt_str_eq_lt_prune';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 20: DateTime column with integer constants (epoch seconds)
-- =====================================================================

-- Integer epoch seconds: 2024-06-15 12:00:00 UTC = 1718452800
SELECT 'dt_int_eq_prune';
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 SETTINGS optimize_and_compare_chain_pruning = 1;

-- Integer epoch conflict
SELECT 'dt_int_eq_conflict';
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_and_compare_chain_pruning = 1;

-- Integer epoch range
SELECT 'dt_int_range';
SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 21: Mixed constant types in same AND expression
-- =====================================================================

-- Int32 column: int constant AND float constant AND string constant → float rewrite: i > 1.5 → i >= 2, pruned by equals
SELECT 'int_mixed_three';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' SETTINGS optimize_and_compare_chain_pruning = 1;

-- Float64 column: string equals AND int less AND float greater
SELECT 'float_mixed_three';
SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- DateTime column: string equals AND integer greater (mixed constant types)
SELECT 'dt_mixed';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 ORDER BY dt SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 22: Integer boundary folding (UInt8: min=0, max=255)
-- =====================================================================

-- constant above max: u = 256 → CONFLICT, folds entire AND to FALSE
SELECT 'boundary_above_max_eq';
SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u > 256 → CONFLICT
SELECT 'boundary_above_max_gt';
SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u < 256 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_above_max_lt';
SELECT * FROM 04032_t WHERE u < 256 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u < 256 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u != 256 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_above_max_ne';
SELECT * FROM 04032_t WHERE u != 256 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u != 256 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u != 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u != 256 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- constant below min: u = -1 → CONFLICT
SELECT 'boundary_below_min_eq';
SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u < -1 → CONFLICT
SELECT 'boundary_below_min_lt';
SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u > -1 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_below_min_gt';
SELECT * FROM 04032_t WHERE u > -1 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u > -1 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > -1 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- constant at max boundary (255 for UInt8):
-- u > 255 → CONFLICT
SELECT 'boundary_at_max_gt';
SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u >= 255 → tightens to u = 255
SELECT 'boundary_at_max_ge';
SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u <= 255 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_at_max_le';
SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- constant at min boundary (0 for UInt8):
-- u < 0 → CONFLICT
SELECT 'boundary_at_min_lt';
SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u <= 0 → tightens to u = 0
SELECT 'boundary_at_min_le';
SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u >= 0 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_at_min_ge';
SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- boundary combined with range on same column
SELECT 'boundary_combined_conflict';
SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_and_compare_chain_pruning = 1;

-- u >= 255 tightens to u = 255, then u != 0 is pruned by equals
SELECT 'boundary_combined_tighten';
SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 ORDER BY u SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 ORDER BY u SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- =====================================================================
-- Section 23: Float literal rewrite for integer columns
-- The rewrite converts float to int internally for comparison analysis,
-- but the AST node is not rewritten. Observable effects: CONFLICT → FALSE,
-- REDUNDANT → condition removed, and correct cross-type comparison.
-- =====================================================================

-- i > 3.5 internally rewritten to i >= 4 for analysis, AST unchanged
SELECT 'float_rewrite_gt';
SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- i < 3.5 internally rewritten to i <= 3 for analysis, AST unchanged
SELECT 'float_rewrite_lt';
SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- i >= 3.5 internally rewritten to i >= 4 for analysis, AST unchanged
SELECT 'float_rewrite_ge';
SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- i <= 3.5 internally rewritten to i <= 3 for analysis, AST unchanged
SELECT 'float_rewrite_le';
SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- i = 3.5 → CONFLICT (3.5 is not an integer), folds entire AND to FALSE
SELECT 'float_rewrite_eq';
SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- i != 3.5 → REDUNDANT (all integers != 3.5), only u > 0 remains
SELECT 'float_rewrite_ne';
SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- exact integer float: i > 3.0 strictly converts to i > 3, AST unchanged
SELECT 'float_rewrite_exact';
SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- float rewrite + boundary: u > 254.5 internally → u >= 255 → u = 255, AST keeps u > 254.5
SELECT 'float_rewrite_boundary';
SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 SETTINGS optimize_and_compare_chain_pruning = 1;

-- float rewrite on both sides: internally i >= 2 AND i <= 4, AST keeps original floats
SELECT 'float_rewrite_range';
SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- two floats on same int column → rewrite i > 3.5 to i >= 4, i < 2.5 to i <= 2 → CONFLICT
SELECT 'float_rewrite_conflict';
SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- two floats on same int column → rewrite i > 1.5 to i >= 2, i > 3.5 to i >= 4 → redundancy, keep i >= 4
SELECT 'float_rewrite_redundant';
SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- two floats narrow to valid range: i > 2.5 → i >= 3, i < 3.5 → i <= 3, both kept (no conflict)
SELECT 'float_rewrite_point';
SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 ORDER BY i SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;

-- int equals vs float greater → rewrite i > 3.5 to i >= 4, conflicts with i = 3 → CONFLICT
SELECT 'float_rewrite_eq_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_and_compare_chain_pruning = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_and_compare_chain_pruning = 1;

DROP TABLE IF EXISTS 04032_t;
