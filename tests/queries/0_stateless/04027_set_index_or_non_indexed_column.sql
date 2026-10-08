-- Tags: no-random-merge-tree-settings

-- Test that set index is not incorrectly used when the predicate doesn't
-- reference any indexed columns but contains `or(..., 0)`.
-- The analyzer may produce such expressions when rewriting OR of equalities
-- on LowCardinality columns: `lc = 'a' OR lc = 'b'` → `or(in(lc, Set), 0)`.
-- The constant `0` (identity element of OR) was incorrectly making
-- `checkDAGUseless` return false.

DROP TABLE IF EXISTS t_set_index_or;

CREATE TABLE t_set_index_or
(
    a UInt64,
    b UInt64,
    INDEX idx_b (b) TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_set_index_or SELECT number, number / 100 FROM numbers(10000);

-- { echoOn }

-- Predicate with or(non_indexed, 0): set index should NOT be used.
SELECT count() FROM t_set_index_or WHERE or(a = 5, 0);
SELECT count() FROM t_set_index_or WHERE or(a = 5, 0) SETTINGS force_data_skipping_indices='idx_b'; -- { serverError INDEX_NOT_USED }

-- Predicate on indexed column: set index should be used.
SELECT count() FROM t_set_index_or WHERE b = 5 SETTINGS force_data_skipping_indices='idx_b';

-- Indexed column inside or(..., 0): set index should still be used.
SELECT count() FROM t_set_index_or WHERE or(b = 5, 0) SETTINGS force_data_skipping_indices='idx_b';

-- { echoOff }

DROP TABLE t_set_index_or;
