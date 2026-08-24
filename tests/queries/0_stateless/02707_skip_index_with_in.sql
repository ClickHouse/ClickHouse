SET explain_query_plan_default = 'legacy';
SET allow_experimental_statistics = 1;
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
DROP TABLE IF EXISTS t_skip_index_in;

CREATE TABLE t_skip_index_in
(
    a String,
    b String,
    c UInt64,
    INDEX idx_c c TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS auto_statistics_types = 'basic';

INSERT INTO t_skip_index_in VALUES ('a', 'b', 1);

set ignore_format_null_for_explain = 0;

-- This query checks that set is not being built if indexes are not used,
-- because with EXPLAIN the set will be built only for analysis of indexes.
-- Materialized statistics must not make this EXPLAIN fail when skip indexes are disabled.
EXPLAIN SELECT count() FROM t_skip_index_in WHERE c IN (SELECT throwIf(1)) SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 1 FORMAT Null;
EXPLAIN SELECT count() FROM t_skip_index_in WHERE c IN (SELECT throwIf(1)) SETTINGS use_skip_indexes = 1, use_statistics = 0, use_statistics_for_part_pruning = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE t_skip_index_in;
