-- Tags: no-random-merge-tree-settings, no-parallel-replicas

-- `exact_rows_before_limit` makes LimitTransform read its source till the end so it can report an exact
-- `rows_before_limit_at_least`. The OFFSET-skip read-in-order optimization must not drop leading granules in
-- that case, or the skipped rows never reach LimitTransform and the count underreports by exactly the rows
-- skipped.

DROP TABLE IF EXISTS t_skip_offset_exact_rows;
CREATE TABLE t_skip_offset_exact_rows (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_skip_offset_exact_rows SELECT number FROM numbers(16);

SELECT k FROM t_skip_offset_exact_rows ORDER BY k LIMIT 3 OFFSET 8
SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0, max_threads = 1,
    optimize_read_in_order = 1, query_plan_optimize_read_in_order_skip_offset = 1
FORMAT JSONCompact;

DROP TABLE t_skip_offset_exact_rows;
