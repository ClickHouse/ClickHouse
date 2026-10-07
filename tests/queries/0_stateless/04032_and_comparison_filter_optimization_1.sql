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
SELECT * FROM 04032_t WHERE i = 3 AND i = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3 SETTINGS optimize_redundant_comparisons = 1;

-- different value → conflict → WHERE = CONSTANT 0
SELECT 'eq_eq_diff';
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 1;

-- constant on left side → conflict → CONSTANT 0
SELECT 'eq_eq_flip';
SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND i = 5 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: Int32 column vs UInt8 constant, same value → redundant
SELECT 'eq_eq_cross_same';
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: Int32 column vs UInt8 constant, different value → conflict
SELECT 'eq_eq_cross_diff';
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5) SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: Int32 column vs Float64 constant, lossless → redundant
SELECT 'eq_eq_float_same';
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: Int32 column vs Float64 constant, lossy → i = 3.5 is always false for Int32 → CONFLICT
SELECT 'eq_eq_float_lossy';
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 2: equals vs less/greater — prune or conflict
-- =====================================================================

-- a = 3 AND a < 5 → prune a < 5, WHERE = single equals
SELECT 'eq_lt_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a < 2 → conflict → CONSTANT 0
SELECT 'eq_lt_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a > 1 → prune a > 1, WHERE = single equals
SELECT 'eq_gt_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a > 5 → conflict → CONSTANT 0
SELECT 'eq_gt_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 5 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a <= 3 → prune a <= 3
SELECT 'eq_le_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a >= 3 → prune a >= 3
SELECT 'eq_ge_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a <= 2 → conflict → CONSTANT 0
SELECT 'eq_le_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i <= 2 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a >= 4 → conflict → CONSTANT 0
SELECT 'eq_ge_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i >= 4 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: equals Int32(3) AND less Float64(3.5) → float rewrite: i < 3.5 → i <= 3, then equals prunes it
SELECT 'eq_lt_cross_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: equals Int32(3) AND less Float64(2.5) → float rewrite: i < 2.5 → i <= 2, conflict with i = 3
SELECT 'eq_lt_cross_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 3: equals vs notEquals
-- =====================================================================

-- a = 3 AND a != 5 → prune a != 5, WHERE = single equals
SELECT 'eq_ne_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i != 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i != 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 5 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a != 3 → conflict → CONSTANT 0
SELECT 'eq_ne_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 4: notEquals vs range — prune notEquals when implied
-- =====================================================================

-- a != 3 AND a < 3 → prune a != 3 (a < 3 implies a != 3)
SELECT 'ne_lt_prune';
SELECT * FROM 04032_t WHERE i != 3 AND i < 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i < 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i < 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i < 3 SETTINGS optimize_redundant_comparisons = 1;

-- a != 3 AND a > 3 → prune a != 3
SELECT 'ne_gt_prune';
SELECT * FROM 04032_t WHERE i != 3 AND i > 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i > 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 1;

-- a != 3 AND a <= 3 → strengthen to a < 3
SELECT 'ne_le_strengthen';
SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a != 3 AND a >= 3 → strengthen to a > 3
SELECT 'ne_ge_strengthen';
SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a != 3 AND a != 3 → redundant notEquals
SELECT 'ne_ne_same';
SELECT * FROM 04032_t WHERE i != 3 AND i != 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i != 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- a != 3 AND a != 5 → keep both (independent)
SELECT 'ne_ne_diff';
SELECT * FROM 04032_t WHERE i != 3 AND i != 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i != 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i != 5 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 5: less vs less — tighter bound wins
-- =====================================================================

-- a < 5 AND a < 3 → keep a < 3 only
SELECT 'lt_lt_tighter';
SELECT * FROM 04032_t WHERE i < 5 AND i < 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 5 AND i < 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i < 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i < 3 SETTINGS optimize_redundant_comparisons = 1;

-- a <= 5 AND a <= 3 → keep a <= 3 only
SELECT 'le_le_tighter';
SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a < 3 AND a <= 3 → keep a < 3 (stricter)
SELECT 'lt_le_same_val';
SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a <= 3 AND a < 3 → keep a < 3 (stricter)
SELECT 'le_lt_same_val';
SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 6: greater vs greater — tighter bound wins
-- =====================================================================

-- a > 1 AND a > 3 → keep a > 3 only
SELECT 'gt_gt_tighter';
SELECT * FROM 04032_t WHERE i > 1 AND i > 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i > 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i > 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i > 3 SETTINGS optimize_redundant_comparisons = 1;

-- a >= 1 AND a >= 3 → keep a >= 3 only
SELECT 'ge_ge_tighter';
SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a > 3 AND a >= 3 → keep a > 3 (stricter)
SELECT 'gt_ge_same_val';
SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 7: less vs greater — range validity
-- =====================================================================

-- a < 5 AND a > 1 → valid range, keep both
SELECT 'lt_gt_valid';
SELECT * FROM 04032_t WHERE i < 5 AND i > 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 5 AND i > 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i > 1 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 5 AND i > 1 SETTINGS optimize_redundant_comparisons = 1;

-- a < 1 AND a > 5 → conflict (empty range) → CONSTANT 0
SELECT 'lt_gt_conflict';
SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 1 AND i > 5 SETTINGS optimize_redundant_comparisons = 1;

-- a <= 3 AND a >= 3 → valid (point), keep both
SELECT 'le_ge_point';
SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a < 3 AND a >= 3 → conflict → CONSTANT 0
SELECT 'lt_ge_conflict';
SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i >= 3 SETTINGS optimize_redundant_comparisons = 1;

-- a <= 3 AND a > 3 → conflict → CONSTANT 0
SELECT 'le_gt_conflict';
SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 1;

-- a < 3 AND a > 3 → conflict → CONSTANT 0
SELECT 'lt_gt_conflict_same';
SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND i > 3 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: less Float vs greater Int, valid range
SELECT 'lt_gt_cross';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 8: constant on left (flip normalization)
-- =====================================================================

SELECT 'flip_less';
SELECT * FROM 04032_t WHERE 5 > i AND i > 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE 5 > i AND i > 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND i > 1 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND i > 1 SETTINGS optimize_redundant_comparisons = 1;

SELECT 'flip_equals_and_range';
SELECT * FROM 04032_t WHERE 3 = i AND 10 > i ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE 3 = i AND 10 > i ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND 10 > i SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 = i AND 10 > i SETTINGS optimize_redundant_comparisons = 1;

SELECT 'flip_both_sides';
SELECT * FROM 04032_t WHERE 5 > i AND 1 < i ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE 5 > i AND 1 < i ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND 1 < i SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 5 > i AND 1 < i SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 9: multiple expressions — independent optimization
-- =====================================================================

SELECT 'multi_expr';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 SETTINGS optimize_redundant_comparisons = 1;

SELECT 'multi_expr_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0 SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE IF EXISTS 04032_t;
