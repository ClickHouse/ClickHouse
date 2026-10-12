DROP TABLE IF EXISTS read_in_order_projection_row_limits SYNC;

CREATE TABLE read_in_order_projection_row_limits
(
    a UInt64,
    b UInt64,
    PROJECTION by_b (SELECT a, b ORDER BY b)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 1;

INSERT INTO read_in_order_projection_row_limits SELECT number, 10 - number FROM numbers(10);

-- `max_rows_to_read` remains enforced when a filter prevents the LIMIT from being pushed to
-- the ordered read.
SELECT a
FROM read_in_order_projection_row_limits
WHERE a >= 0
ORDER BY b
LIMIT 1
SETTINGS optimize_use_projections = 1, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }

DROP TABLE read_in_order_projection_row_limits SYNC;
