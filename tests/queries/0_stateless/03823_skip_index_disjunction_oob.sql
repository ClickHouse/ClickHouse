-- Reproducer for out-of-bounds access in partial_disjunction_result bitset.
-- When use_primary_key = 0, the key_condition RPN has 1 element (skip_analysis_),
-- so the check key_condition.getRPN().size() <= 32 passes trivially.
-- But the key_condition_rpn_template and index conditions have the full RPN,
-- which can exceed 32 elements, leading to out-of-bounds writes in the callback.
-- The disjunction feature requires at least 2 useful skip indexes and OR conditions.

DROP TABLE IF EXISTS t_skip_index_disj_oob;

CREATE TABLE t_skip_index_disj_oob
(
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax,
    INDEX idx_b b TYPE minmax
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_skip_index_disj_oob SELECT number, number FROM numbers(200);

-- 9 groups of (a = X AND b = X) connected by OR produce 35 RPN elements:
-- 9 * 3 (leaf, leaf, AND) + 8 OR = 35, exceeding the limit of 32.
-- optimize_min_equality_disjunction_chain_length = 100 prevents OR-to-IN conversion.
SELECT count() FROM t_skip_index_disj_oob
WHERE (a = 1 AND b = 1) OR (a = 2 AND b = 2) OR (a = 3 AND b = 3)
   OR (a = 4 AND b = 4) OR (a = 5 AND b = 5) OR (a = 6 AND b = 6)
   OR (a = 7 AND b = 7) OR (a = 8 AND b = 8) OR (a = 9 AND b = 9)
SETTINGS use_primary_key = 0, use_skip_indexes_on_data_read = 0,
         use_skip_indexes_for_disjunctions = 1, use_query_condition_cache = 0,
         optimize_min_equality_disjunction_chain_length = 100;

SELECT count() FROM t_skip_index_disj_oob
WHERE (a = 1 AND b = 1) OR (a = 2 AND b = 2) OR (a = 3 AND b = 3)
   OR (a = 4 AND b = 4) OR (a = 5 AND b = 5) OR (a = 6 AND b = 6)
   OR (a = 7 AND b = 7) OR (a = 8 AND b = 8) OR (a = 9 AND b = 9)
SETTINGS use_primary_key = 0, use_skip_indexes_on_data_read = 1,
         use_skip_indexes_for_disjunctions = 1, use_query_condition_cache = 0,
         optimize_min_equality_disjunction_chain_length = 100;

DROP TABLE t_skip_index_disj_oob;
