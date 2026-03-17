SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;

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
SELECT * FROM 04032_t WHERE i = 3 AND i = 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = 3;

-- different value → conflict → WHERE = CONSTANT 0
SELECT 'eq_eq_diff';
SELECT * FROM 04032_t WHERE i = 3 AND i = 5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = 5;

-- constant on left side → conflict → CONSTANT 0
SELECT 'eq_eq_flip';
SELECT * FROM 04032_t WHERE 3 = i AND i = 5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE 3 = i AND i = 5;

-- cross-type: Int32 column vs UInt8 constant, same value → redundant
SELECT 'eq_eq_cross_same';
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3) ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(3);

-- cross-type: Int32 column vs UInt8 constant, different value → conflict
SELECT 'eq_eq_cross_diff';
SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5);
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = toUInt8(5);

-- cross-type: Int32 column vs Float64 constant, lossless → redundant
SELECT 'eq_eq_float_same';
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = 3.0;

-- cross-type: Int32 column vs Float64 constant, lossy (not convertible to Int32)
-- should NOT optimize (skip safely), WHERE = AND with both conditions
SELECT 'eq_eq_float_lossy';
SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = 3.5;

-- =====================================================================
-- Section 2: equals vs less/greater — prune or conflict
-- =====================================================================

-- a = 3 AND a < 5 → prune a < 5, WHERE = single equals
SELECT 'eq_lt_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i < 5;

-- a = 3 AND a < 2 → conflict → CONSTANT 0
SELECT 'eq_lt_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 2;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i < 2;

-- a = 3 AND a > 1 → prune a > 1, WHERE = single equals
SELECT 'eq_gt_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i > 1;

-- a = 3 AND a > 5 → conflict → CONSTANT 0
SELECT 'eq_gt_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i > 5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i > 5;

-- a = 3 AND a <= 3 → prune a <= 3
SELECT 'eq_le_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i <= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i <= 3;

-- a = 3 AND a >= 3 → prune a >= 3
SELECT 'eq_ge_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i >= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i >= 3;

-- a = 3 AND a <= 2 → conflict → CONSTANT 0
SELECT 'eq_le_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i <= 2;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i <= 2;

-- a = 3 AND a >= 4 → conflict → CONSTANT 0
SELECT 'eq_ge_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i >= 4;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i >= 4;

-- cross-type: equals Int32(3) AND less Float64(3.5) → lossy, skip optimization, keep AND
SELECT 'eq_lt_cross_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i < 3.5;

-- cross-type: equals Int32(3) AND less Float64(2.5) → lossy, skip optimization, keep AND
SELECT 'eq_lt_cross_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i < 2.5;

-- =====================================================================
-- Section 3: equals vs notEquals
-- =====================================================================

-- a = 3 AND a != 5 → prune a != 5, WHERE = single equals
SELECT 'eq_ne_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i != 5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i != 5;

-- a = 3 AND a != 3 → conflict → CONSTANT 0
SELECT 'eq_ne_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i != 3;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i != 3;

-- =====================================================================
-- Section 4: notEquals vs range — prune notEquals when implied
-- =====================================================================

-- a != 3 AND a < 3 → prune a != 3 (a < 3 implies a != 3)
SELECT 'ne_lt_prune';
SELECT * FROM 04032_t WHERE i != 3 AND i < 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 3 AND i < 3;

-- a != 3 AND a > 3 → prune a != 3
SELECT 'ne_gt_prune';
SELECT * FROM 04032_t WHERE i != 3 AND i > 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 3 AND i > 3;

-- a != 3 AND a <= 3 → keep both (a could be 2)
SELECT 'ne_le_keep';
SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 3 AND i <= 3;

-- a != 3 AND a >= 3 → keep both (a could be 4)
SELECT 'ne_ge_keep';
SELECT * FROM 04032_t WHERE i != 3 AND i >= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 3 AND i >= 3;

-- a != 3 AND a != 3 → redundant notEquals
SELECT 'ne_ne_same';
SELECT * FROM 04032_t WHERE i != 3 AND i != 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 3 AND i != 3;

-- a != 3 AND a != 5 → keep both (independent)
SELECT 'ne_ne_diff';
SELECT * FROM 04032_t WHERE i != 3 AND i != 5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 3 AND i != 5;

-- =====================================================================
-- Section 5: less vs less — tighter bound wins
-- =====================================================================

-- a < 5 AND a < 3 → keep a < 3 only
SELECT 'lt_lt_tighter';
SELECT * FROM 04032_t WHERE i < 5 AND i < 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i < 5 AND i < 3;

-- a <= 5 AND a <= 3 → keep a <= 3 only
SELECT 'le_le_tighter';
SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i <= 5 AND i <= 3;

-- a < 3 AND a <= 3 → keep a < 3 (stricter)
SELECT 'lt_le_same_val';
SELECT * FROM 04032_t WHERE i < 3 AND i <= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i < 3 AND i <= 3;

-- a <= 3 AND a < 3 → keep a < 3 (stricter)
SELECT 'le_lt_same_val';
SELECT * FROM 04032_t WHERE i <= 3 AND i < 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i <= 3 AND i < 3;

-- =====================================================================
-- Section 6: greater vs greater — tighter bound wins
-- =====================================================================

-- a > 1 AND a > 3 → keep a > 3 only
SELECT 'gt_gt_tighter';
SELECT * FROM 04032_t WHERE i > 1 AND i > 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > 1 AND i > 3;

-- a >= 1 AND a >= 3 → keep a >= 3 only
SELECT 'ge_ge_tighter';
SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i >= 1 AND i >= 3;

-- a > 3 AND a >= 3 → keep a > 3 (stricter)
SELECT 'gt_ge_same_val';
SELECT * FROM 04032_t WHERE i > 3 AND i >= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > 3 AND i >= 3;

-- =====================================================================
-- Section 7: less vs greater — range validity
-- =====================================================================

-- a < 5 AND a > 1 → valid range, keep both
SELECT 'lt_gt_valid';
SELECT * FROM 04032_t WHERE i < 5 AND i > 1 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i < 5 AND i > 1;

-- a < 1 AND a > 5 → conflict (empty range) → CONSTANT 0
SELECT 'lt_gt_conflict';
SELECT * FROM 04032_t WHERE i < 1 AND i > 5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i < 1 AND i > 5;

-- a <= 3 AND a >= 3 → valid (point), keep both
SELECT 'le_ge_point';
SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i <= 3 AND i >= 3;

-- a < 3 AND a >= 3 → conflict → CONSTANT 0
SELECT 'lt_ge_conflict';
SELECT * FROM 04032_t WHERE i < 3 AND i >= 3;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i < 3 AND i >= 3;

-- a <= 3 AND a > 3 → conflict → CONSTANT 0
SELECT 'le_gt_conflict';
SELECT * FROM 04032_t WHERE i <= 3 AND i > 3;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i <= 3 AND i > 3;

-- a < 3 AND a > 3 → conflict → CONSTANT 0
SELECT 'lt_gt_conflict_same';
SELECT * FROM 04032_t WHERE i < 3 AND i > 3;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i < 3 AND i > 3;

-- cross-type: less Float vs greater Int, valid range
SELECT 'lt_gt_cross';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > 1 AND i < 5.5;

-- =====================================================================
-- Section 8: constant on left (flip normalization)
-- =====================================================================

SELECT 'flip_less';
SELECT * FROM 04032_t WHERE 5 > i AND i > 1 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE 5 > i AND i > 1;

SELECT 'flip_equals_and_range';
SELECT * FROM 04032_t WHERE 3 = i AND 10 > i ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE 3 = i AND 10 > i;

SELECT 'flip_both_sides';
SELECT * FROM 04032_t WHERE 5 > i AND 1 < i ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE 5 > i AND 1 < i;

-- =====================================================================
-- Section 9: multiple expressions — independent optimization
-- =====================================================================

SELECT 'multi_expr';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND f > 2.0 AND f < 6.0;

SELECT 'multi_expr_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = 5 AND f > 1.0;

-- =====================================================================
-- Section 10: three+ filters on same expression
-- =====================================================================

-- a = 3 AND a > 1 AND a < 5 → prune range, keep a = 3 only
SELECT 'eq_range_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5;

-- a > 1 AND a < 5 AND a > 3 → tighten to a > 3 AND a < 5
SELECT 'range_tighten';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3;

-- a = 3 AND a < 5 AND a > 5 → conflict → CONSTANT 0
SELECT 'three_way_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5;

-- =====================================================================
-- Section 11: LowCardinality and String columns
-- =====================================================================

SELECT 'lc_eq_eq';
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y';

SELECT 'lc_eq_conflict';
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z';
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z';

SELECT 'str_eq_eq';
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c';

SELECT 'str_eq_conflict';
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a';
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a';

-- =====================================================================
-- Section 12: transitive inference + conflict detection (commit d4c0509a0c3)
-- =====================================================================

-- x = 3 AND x = y AND y = 5 → transitive infers x=5, then conflict x=3 vs x=5 → FALSE
SELECT 'transitive_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5;

-- x < 3 AND y > 3 AND y < 10 → should NOT produce redundant x < 10
SELECT 'transitive_no_redundant';
SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10;

-- a > b AND b > 3 → infers a > 3
SELECT 'transitive_infer';
SELECT * FROM 04032_t WHERE i > u AND u > 2 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > u AND u > 2;

-- chain: a = b AND b = 3 → infers a = 3
SELECT 'transitive_eq_chain';
SELECT * FROM 04032_t WHERE i = u AND u = 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = u AND u = 3;

-- =====================================================================
-- Section 13: non-comparison operands preserved
-- =====================================================================

SELECT 'mixed_operands';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '';

-- =====================================================================
-- Section 14: notEquals chain → NOT IN conversion
-- =====================================================================

SET optimize_min_inequality_conjunction_chain_length = 2;

SELECT 'ne_chain_not_in';
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7;

-- notEquals chain with redundant duplicate
SELECT 'ne_chain_dedup';
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1;

-- =====================================================================
-- Section 15: edge cases
-- =====================================================================

-- single comparison (no optimization needed)
SELECT 'single';
SELECT * FROM 04032_t WHERE i = 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3;

-- non-constant expression (not optimizable), WHERE = AND preserved
SELECT 'non_const';
SELECT * FROM 04032_t WHERE i > u AND i < u ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > u AND i < u;

-- NULL handling: comparison with NULL should not crash
SELECT 'null_safe';
SELECT * FROM 04032_t WHERE i = 3 AND i > NULL;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i > NULL;

-- =====================================================================
-- Section 16: Mixed types — string constants vs Int32 column
-- =====================================================================

-- String '3' parseable as Int32, same value as 3 → redundant
SELECT 'int_str_eq_same';
SELECT * FROM 04032_t WHERE i = '3' AND i = 3 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = '3' AND i = 3;

-- String constants '3' vs '5' on Int32 column → conflict
SELECT 'int_str_eq_diff';
SELECT * FROM 04032_t WHERE i = '3' AND i = '5';
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = '3' AND i = '5';

-- String '3.5' cannot convert to Int32 → runtime TYPE_MISMATCH error
SELECT * FROM 04032_t WHERE i = 3 AND i = '3.5'; -- { serverError TYPE_MISMATCH }

-- String '3.0' cannot convert to Int32 → runtime TYPE_MISMATCH error
SELECT * FROM 04032_t WHERE i = '3.0' AND i = 3; -- { serverError TYPE_MISMATCH }

-- String range on Int32 column
SELECT 'int_str_range';
SELECT * FROM 04032_t WHERE i > '1' AND i < '5' ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i > '1' AND i < '5';

-- =====================================================================
-- Section 17: Mixed types — integer constants vs Float64 column
-- =====================================================================

-- Int 3 converts losslessly to Float64(3.0), same as 3.0 → redundant
SELECT 'float_int_eq_same';
SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0;

-- Int 3 vs Int 4 on Float64 column → conflict
SELECT 'float_int_eq_diff';
SELECT * FROM 04032_t WHERE f = 3 AND f = 4;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f = 3 AND f = 4;

-- Int range on Float64 column
SELECT 'float_int_range';
SELECT * FROM 04032_t WHERE f > 1 AND f < 5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f > 1 AND f < 5;

-- Int equals and Float less on Float64 column → prune
SELECT 'float_int_eq_float_lt';
SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5;

-- =====================================================================
-- Section 18: Mixed types — string constants vs Float64 column
-- =====================================================================

-- String '1.5' parses to Float64(1.5), same value → redundant
SELECT 'float_str_eq_same';
SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5;

-- String '3.0' and int less on Float64 column → prune
SELECT 'float_str_and_int';
SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5;

-- String '3.0' vs String '5.0' on Float64 column → conflict
SELECT 'float_str_eq_diff';
SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0';
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0';

-- String range on Float64 column
SELECT 'float_str_range';
SELECT * FROM 04032_t WHERE f > '1' AND f < '5' ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f > '1' AND f < '5';

-- =====================================================================
-- Section 19: DateTime column with string constants
-- =====================================================================

-- Same DateTime string → redundant
SELECT 'dt_str_eq_same';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' ORDER BY dt;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00';

-- Different DateTime strings → conflict
SELECT 'dt_str_eq_diff';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00';
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00';

-- DateTime string range — valid
SELECT 'dt_str_range';
SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00';

-- DateTime equals + greater conflict
SELECT 'dt_str_eq_gt_conflict';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00';
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00';

-- DateTime equals + less prune
SELECT 'dt_str_eq_lt_prune';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00';

-- =====================================================================
-- Section 20: DateTime column with integer constants (epoch seconds)
-- =====================================================================

-- Integer epoch seconds: 2024-06-15 12:00:00 UTC = 1718452800
SELECT 'dt_int_eq_prune';
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 ORDER BY dt;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200;

-- Integer epoch conflict
SELECT 'dt_int_eq_conflict';
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600;

-- Integer epoch range
SELECT 'dt_int_range';
SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 ORDER BY dt;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600;

-- =====================================================================
-- Section 21: Mixed constant types in same AND expression
-- =====================================================================

-- Int32 column: int constant AND float constant AND string constant
SELECT 'int_mixed_three';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5';

-- Float64 column: string equals AND int less AND float greater
SELECT 'float_mixed_three';
SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 ORDER BY i;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5;

-- DateTime column: string equals AND integer greater (mixed constant types)
SELECT 'dt_mixed';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 ORDER BY dt;
EXPLAIN QUERY TREE SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200;

DROP TABLE IF EXISTS 04032_t;
