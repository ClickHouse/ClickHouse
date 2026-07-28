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
-- Section 1: Mixed types — string constants vs Int32 column
-- =====================================================================

-- String '3' parseable as Int32, same value as 3 → redundant
SELECT 'int_str_eq_same';
SELECT * FROM 04032_t WHERE i = '3' AND i = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = '3' AND i = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = 3 SETTINGS optimize_redundant_comparisons = 1;

-- String constants '3' vs '5' on Int32 column → conflict
SELECT 'int_str_eq_diff';
SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = '3' AND i = '5' SETTINGS optimize_redundant_comparisons = 1;

-- String range on Int32 column
SELECT 'int_str_range';
SELECT * FROM 04032_t WHERE i > '1' AND i < '5' ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > '1' AND i < '5' ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > '1' AND i < '5' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > '1' AND i < '5' SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 2: Mixed types — integer constants vs Float64 column
-- =====================================================================

-- Int 3 converts losslessly to Float64(3.0), same as 3.0 → redundant
SELECT 'float_int_eq_same';
SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 3.0 SETTINGS optimize_redundant_comparisons = 1;

-- Int 3 vs Int 4 on Float64 column → conflict
SELECT 'float_int_eq_diff';
SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f = 4 SETTINGS optimize_redundant_comparisons = 1;

-- Int range on Float64 column
SELECT 'float_int_range';
SELECT * FROM 04032_t WHERE f > 1 AND f < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f > 1 AND f < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > 1 AND f < 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > 1 AND f < 5 SETTINGS optimize_redundant_comparisons = 1;

-- Int equals and Float less on Float64 column → prune
SELECT 'float_int_eq_float_lt';
SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = 3 AND f < 3.5 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 3: Mixed types — string constants vs Float64 column
-- =====================================================================

-- String '1.5' parses to Float64(1.5), same value → redundant
SELECT 'float_str_eq_same';
SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '1.5' AND f = 1.5 SETTINGS optimize_redundant_comparisons = 1;

-- String '3.0' and int less on Float64 column → prune
SELECT 'float_str_and_int';
SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f < 5 SETTINGS optimize_redundant_comparisons = 1;

-- String '3.0' vs String '5.0' on Float64 column → conflict
SELECT 'float_str_eq_diff';
SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f = '5.0' SETTINGS optimize_redundant_comparisons = 1;

-- String range on Float64 column
SELECT 'float_str_range';
SELECT * FROM 04032_t WHERE f > '1' AND f < '5' ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f > '1' AND f < '5' ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > '1' AND f < '5' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f > '1' AND f < '5' SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 4: DateTime column with string constants
-- =====================================================================

-- Same DateTime string → redundant
SELECT 'dt_str_eq_same';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' ORDER BY dt SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' ORDER BY dt SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2024-06-15 12:00:00' SETTINGS optimize_redundant_comparisons = 1;

-- Different DateTime strings → conflict
SELECT 'dt_str_eq_diff';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt = '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 1;

-- DateTime string range — valid
SELECT 'dt_str_range';
SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > '2024-01-01 00:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 1;

-- DateTime equals + greater conflict
SELECT 'dt_str_eq_gt_conflict';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 1;

-- DateTime equals + less prune
SELECT 'dt_str_eq_lt_prune';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' ORDER BY dt SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt < '2025-01-01 00:00:00' SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 5: DateTime column with integer constants (epoch seconds)
-- =====================================================================

-- Integer epoch seconds: 2024-06-15 12:00:00 UTC = 1718452800
SELECT 'dt_int_eq_prune';
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 ORDER BY dt SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 ORDER BY dt SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt > 1704067200 SETTINGS optimize_redundant_comparisons = 1;

-- Integer epoch conflict
SELECT 'dt_int_eq_conflict';
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = 1718452800 AND dt = 1735689600 SETTINGS optimize_redundant_comparisons = 1;

-- Integer epoch range
SELECT 'dt_int_range';
SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 ORDER BY dt SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 ORDER BY dt SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt > 1704067200 AND dt < 1735689600 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 6: Mixed constant types in same AND expression
-- =====================================================================

-- Int32 column: int constant AND float constant AND string constant → float rewrite: i > 1.5 → i >= 2, pruned by equals
SELECT 'int_mixed_three';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1.5 AND i < '5' SETTINGS optimize_redundant_comparisons = 1;

-- Float64 column: string equals AND int less AND float greater
SELECT 'float_mixed_three';
SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f = '3.0' AND f > 1 AND f < 5.5 SETTINGS optimize_redundant_comparisons = 1;

-- DateTime column: string equals AND integer greater (mixed constant types)
SELECT 'dt_mixed';
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 ORDER BY dt SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 ORDER BY dt SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE dt = '2024-06-15 12:00:00' AND dt > 1704067200 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 7: Integer boundary folding (UInt8: min=0, max=255)
-- =====================================================================

-- constant above max: u = 256 → CONFLICT, folds entire AND to FALSE
SELECT 'boundary_above_max_eq';
SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u > 256 → CONFLICT
SELECT 'boundary_above_max_gt';
SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u < 256 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_above_max_lt';
SELECT * FROM 04032_t WHERE u < 256 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u < 256 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u != 256 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_above_max_ne';
SELECT * FROM 04032_t WHERE u != 256 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u != 256 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u != 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u != 256 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- constant below min: u = -1 → CONFLICT
SELECT 'boundary_below_min_eq';
SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u = -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u < -1 → CONFLICT
SELECT 'boundary_below_min_lt';
SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u > -1 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_below_min_gt';
SELECT * FROM 04032_t WHERE u > -1 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u > -1 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > -1 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- constant at max boundary (255 for UInt8):
-- u > 255 → CONFLICT
SELECT 'boundary_at_max_gt';
SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u >= 255 → tightens to u = 255
SELECT 'boundary_at_max_ge';
SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u <= 255 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_at_max_le';
SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- constant at min boundary (0 for UInt8):
-- u < 0 → CONFLICT
SELECT 'boundary_at_min_lt';
SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u < 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u <= 0 → tightens to u = 0
SELECT 'boundary_at_min_le';
SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u <= 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- u >= 0 → REDUNDANT (always true), only i > 0 remains
SELECT 'boundary_at_min_ge';
SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- boundary combined with range on same column
SELECT 'boundary_combined_conflict';
SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 255 AND u < 100 SETTINGS optimize_redundant_comparisons = 1;

-- u >= 255 tightens to u = 255, then u != 0 is pruned by equals
SELECT 'boundary_combined_tighten';
SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 ORDER BY u SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 ORDER BY u SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 255 AND u != 0 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 8: Float literal rewrite for integer columns
-- The rewrite converts float to int internally for comparison analysis,
-- but the AST node is not rewritten. Observable effects: CONFLICT → FALSE,
-- REDUNDANT → condition removed, and correct cross-type comparison.
-- =====================================================================

-- i > 3.5 internally rewritten to i >= 4 for analysis, AST unchanged
SELECT 'float_rewrite_gt';
SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;

-- i < 3.5 internally rewritten to i <= 3 for analysis, AST unchanged
SELECT 'float_rewrite_lt';
SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;

-- i >= 3.5 internally rewritten to i >= 4 for analysis, AST unchanged
SELECT 'float_rewrite_ge';
SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;

-- i <= 3.5 internally rewritten to i <= 3 for analysis, AST unchanged
SELECT 'float_rewrite_le';
SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;

-- i = 3.5 → CONFLICT (3.5 is not an integer), folds entire AND to FALSE
SELECT 'float_rewrite_eq';
SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;

-- i != 3.5 → REDUNDANT (all integers != 3.5), only u > 0 remains
SELECT 'float_rewrite_ne';
SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3.5 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;

-- exact integer float: i > 3.0 strictly converts to i > 3, AST unchanged
SELECT 'float_rewrite_exact';
SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.0 AND u > 0 SETTINGS optimize_redundant_comparisons = 1;

-- float rewrite + boundary: u > 254.5 internally → u >= 255 → u = 255, AST keeps u > 254.5
SELECT 'float_rewrite_boundary';
SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u > 254.5 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- float rewrite on both sides: internally i >= 2 AND i <= 4, AST keeps original floats
SELECT 'float_rewrite_range';
SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i < 4.5 SETTINGS optimize_redundant_comparisons = 1;

-- two floats on same int column → rewrite i > 3.5 to i >= 4, i < 2.5 to i <= 2 → CONFLICT
SELECT 'float_rewrite_conflict';
SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 3.5 AND i < 2.5 SETTINGS optimize_redundant_comparisons = 1;

-- two floats on same int column → rewrite i > 1.5 to i >= 2, i > 3.5 to i >= 4 → redundancy, keep i >= 4
SELECT 'float_rewrite_redundant';
SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1.5 AND i > 3.5 SETTINGS optimize_redundant_comparisons = 1;

-- two floats narrow to valid range: i > 2.5 → i >= 3, i < 3.5 → i <= 3, both kept (no conflict)
SELECT 'float_rewrite_point';
SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 2.5 AND i < 3.5 SETTINGS optimize_redundant_comparisons = 1;

-- int equals vs float greater → rewrite i > 3.5 to i >= 4, conflicts with i = 3 → CONFLICT
SELECT 'float_rewrite_eq_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 3.5 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 9: all conditions redundant → replace AND with true
-- =====================================================================

-- u >= 0 AND u < 300 → both always true for UInt8 (range 0..255), result should be constant true
SELECT 'all_redundant_true';
SELECT * FROM 04032_t WHERE u >= 0 AND u < 300 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u >= 0 AND u < 300 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND u < 300 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND u < 300 SETTINGS optimize_redundant_comparisons = 1;

-- u >= 0 AND u <= 255 AND u != 256 → all three always true for UInt8
SELECT 'all_redundant_three';
SELECT * FROM 04032_t WHERE u >= 0 AND u <= 255 AND u != 256 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE u >= 0 AND u <= 255 AND u != 256 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND u <= 255 AND u != 256 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE u >= 0 AND u <= 255 AND u != 256 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 10: notEquals + inclusive range strengthening
-- =====================================================================

-- constant on left side: 3 >= i AND i != 3 → i < 3
SELECT 'ne_le_strengthen_flip';
SELECT * FROM 04032_t WHERE 3 >= i AND i != 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE 3 >= i AND i != 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 >= i AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 >= i AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- constant on left side: 3 <= i AND i != 3 → i > 3
SELECT 'ne_ge_strengthen_flip';
SELECT * FROM 04032_t WHERE 3 <= i AND i != 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE 3 <= i AND i != 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 <= i AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3 <= i AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- different values: i != 5 AND i <= 3 → no strengthening, != is pruned (5 > 3)
SELECT 'ne_le_diff_prune';
SELECT * FROM 04032_t WHERE i != 5 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 5 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 5 AND i <= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 5 AND i <= 3 SETTINGS optimize_redundant_comparisons = 1;

-- strengthen + additional range: i != 3 AND i <= 3 AND i >= 1 → i < 3 AND i >= 1
SELECT 'ne_le_strengthen_with_range';
SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 AND i >= 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 AND i >= 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 AND i >= 1 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i <= 3 AND i >= 1 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type: i != toUInt8(3) AND i <= 3 → strengthen to i < 3
SELECT 'ne_le_strengthen_cross_type';
SELECT * FROM 04032_t WHERE i != toUInt8(3) AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != toUInt8(3) AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != toUInt8(3) AND i <= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != toUInt8(3) AND i <= 3 SETTINGS optimize_redundant_comparisons = 1;

-- float column: f != 3.0 AND f <= 3.0 → f < 3.0
SELECT 'ne_le_strengthen_float';
SELECT * FROM 04032_t WHERE f != 3.0 AND f <= 3.0 ORDER BY f SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f != 3.0 AND f <= 3.0 ORDER BY f SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f != 3.0 AND f <= 3.0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f != 3.0 AND f <= 3.0 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type int col: != 3.0 AND <= 3 (float literal with int range) → i < 3
SELECT 'ne_le_strengthen_float_literal';
SELECT * FROM 04032_t WHERE i != 3.0 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3.0 AND i <= 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3.0 AND i <= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3.0 AND i <= 3 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type int col: != 3 AND >= 3.0 (int literal with float range) → i > 3.0
SELECT 'ne_ge_strengthen_float_range';
SELECT * FROM 04032_t WHERE i != 3 AND i >= 3.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 3 AND i >= 3.0 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i >= 3.0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 3 AND i >= 3.0 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type float col: f != 3 AND f <= 3.0 (int literal != with float range) → f < 3.0
SELECT 'ne_le_strengthen_float_col_int_ne';
SELECT * FROM 04032_t WHERE f != 3 AND f <= 3.0 ORDER BY f SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f != 3 AND f <= 3.0 ORDER BY f SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f != 3 AND f <= 3.0 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f != 3 AND f <= 3.0 SETTINGS optimize_redundant_comparisons = 1;

-- cross-type float col: f != 3.0 AND f >= 3 (float != with int range) → f > 3
SELECT 'ne_ge_strengthen_float_col_int_range';
SELECT * FROM 04032_t WHERE f != 3.0 AND f >= 3 ORDER BY f SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE f != 3.0 AND f >= 3 ORDER BY f SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f != 3.0 AND f >= 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE f != 3.0 AND f >= 3 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 11: float-rewrite on int columns combined with downstream comparisons (notEquals strengthening, PRUNE/NONE, boundary fold).
-- =====================================================================

-- 11.A  Float-rewrite + notEquals → STRENGTHEN (the actual rebuild path; was the original Bug 1).

-- LESS rewritten to LESS_OR_EQUALS, then strengthened back to LESS.
SELECT 'float_rewrite_lt_ne_strengthen';
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- LESS_OR_EQUALS already; the rewrite only swaps the constant.
SELECT 'float_rewrite_le_ne_strengthen';
SELECT groupArray(i) FROM 04032_t WHERE i <= 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i <= 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i <= 3.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- GREATER rewritten via ceil to GREATER_OR_EQUALS, then strengthened back to GREATER.
SELECT 'float_rewrite_gt_ne_strengthen';
SELECT groupArray(i) FROM 04032_t WHERE i > 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i > 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- GREATER_OR_EQUALS already; ceil rewrite, then notEquals strengthens.
SELECT 'float_rewrite_ge_ne_strengthen';
SELECT groupArray(i) FROM 04032_t WHERE i >= 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i >= 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i >= 2.5 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- Constant-on-left flip: same logical case as `i < 3.5 AND i != 3`.
SELECT 'float_rewrite_lt_ne_strengthen_flip';
SELECT groupArray(i) FROM 04032_t WHERE 3.5 > i AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE 3.5 > i AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3.5 > i AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE 3.5 > i AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- 11.B  Float-rewrite + chained notEquals → cumulative strengthening (`i < 4.5 AND i != 4 AND i != 3` → `i < 4 AND i != 3`).
SELECT 'float_rewrite_lt_ne_chain';
SELECT groupArray(i) FROM 04032_t WHERE i < 4.5 AND i != 4 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < 4.5 AND i != 4 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 4.5 AND i != 4 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 4.5 AND i != 4 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- Mirror: `i > 0.5` → `i >= 1`; `i != 1` strengthens to `i > 1`; `i != 3` independent.
SELECT 'float_rewrite_gt_ne_chain';
SELECT groupArray(i) FROM 04032_t WHERE i > 0.5 AND i != 1 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i > 0.5 AND i != 1 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 0.5 AND i != 1 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 0.5 AND i != 1 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- 11.C  Float-rewrite + non-strengthening compare ops (PRUNE / NONE; rewrite must not corrupt these branches).

-- Float-rewrite + EQUALS at the same value → equals subsumes the range (PRUNE).
SELECT 'float_rewrite_lt_eq_prune';
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i = 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i = 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i = 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i = 3 SETTINGS optimize_redundant_comparisons = 1;

-- Float-rewrite + same-direction tighter range → tighter range wins (PRUNE), float operand erased.
SELECT 'float_rewrite_lt_lt_prune';
SELECT groupArray(i) FROM 04032_t WHERE i < 4.5 AND i < 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < 4.5 AND i < 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 4.5 AND i < 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 4.5 AND i < 3 SETTINGS optimize_redundant_comparisons = 1;

-- Float-rewrite is the surviving (looser) range; the integer companion is pruned.
-- AST is not rebuilt, so EXPLAIN keeps the original `i < 3.5` literal (semantically
-- equivalent to `i <= 3` for an Int32 column at runtime).
SELECT 'float_rewrite_lt_lt_keep';
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i < 100 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i < 100 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i < 100 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i < 100 SETTINGS optimize_redundant_comparisons = 1;

-- Float-rewrite + opposite-direction non-conflicting range → both kept (NONE).
SELECT 'float_rewrite_lt_gt_keep_both';
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i > 1 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < 3.5 AND i > 1 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i > 1 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3.5 AND i > 1 SETTINGS optimize_redundant_comparisons = 1;

-- 11.D  Float-rewrite + boundary fold (CONFLICT / REDUNDANT bypassing the pair-comparison machinery).

-- `i < -1e10` on Int32 is below the type minimum → CONFLICT → whole AND folds to 0.
SELECT 'float_rewrite_below_min_conflict';
SELECT groupArray(i) FROM 04032_t WHERE i < -1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < -1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < -1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < -1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- `i < 1e10` on Int32 is above the type maximum → REDUNDANT → only `i != 3` survives.
SELECT 'float_rewrite_above_max_redundant';
SELECT groupArray(i) FROM 04032_t WHERE i < 1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT groupArray(i) FROM 04032_t WHERE i < 1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 1e10 AND i != 3 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 12: a contradiction must not suppress an exception from a sibling operand.
-- When the contradictory chain also contains a comparison whose constant cannot be losslessly
-- converted to the column type (e.g. `i > 'str'`), that comparison raises `TYPE_MISMATCH` at
-- runtime. The optimization must not fold the AND to `0` and silently drop it, so the query
-- throws identically with the optimization on and off.
-- =====================================================================

-- TYPE_MISMATCH on `i > 'str'`: must be thrown whether or not the optimization is enabled.
SELECT 'conflict_folds_throwing_operand_type_mismatch_off';
SELECT groupArray(i) FROM 04032_t WHERE i > 'str' AND i < 0 AND i > 10 SETTINGS optimize_redundant_comparisons = 0; -- { serverError TYPE_MISMATCH }
SELECT 'conflict_folds_throwing_operand_type_mismatch_on';
SELECT groupArray(i) FROM 04032_t WHERE i > 'str' AND i < 0 AND i > 10 SETTINGS optimize_redundant_comparisons = 1; -- { serverError TYPE_MISMATCH }
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 'str' AND i < 0 AND i > 10 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 'str' AND i < 0 AND i > 10 SETTINGS optimize_redundant_comparisons = 1;

-- Same shape with EQUALS: must throw whether or not the optimization is enabled.
SELECT 'conflict_folds_throwing_operand_eq_off';
SELECT groupArray(i) FROM 04032_t WHERE i = 'str' AND i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 0; -- { serverError TYPE_MISMATCH }
SELECT 'conflict_folds_throwing_operand_eq_on';
SELECT groupArray(i) FROM 04032_t WHERE i = 'str' AND i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 1; -- { serverError TYPE_MISMATCH }
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 'str' AND i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 'str' AND i = 3 AND i = 5 SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE IF EXISTS 04032_t;
