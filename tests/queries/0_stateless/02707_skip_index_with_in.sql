DROP TABLE IF EXISTS t_skip_index_in;

CREATE TABLE t_skip_index_in
(
    a String,
    b String,
    c String,
    INDEX idx_c c TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (a, b);

INSERT INTO t_skip_index_in VALUES ('a', 'b', 'c');

-- This query checks that set is not being built if indexes are not used,
-- because with EXPLAIN the set will be built only for analysis of indexes.
EXPLAIN SELECT count() FROM t_skip_index_in WHERE c IN (SELECT throwIf(1)) SETTINGS use_skip_indexes = 0 FORMAT Null;
EXPLAIN SELECT count() FROM t_skip_index_in WHERE c IN (SELECT throwIf(1)) SETTINGS use_skip_indexes = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE t_skip_index_in;
