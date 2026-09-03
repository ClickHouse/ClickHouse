-- compatibility = '26.6' must keep sliding frames on the recompute path: on this
-- rounding-sensitive pattern the tree's re-associated float sums differ bitwise
-- from the sequential ones, so both configurations must produce identical bits.
DROP TABLE IF EXISTS window_compat_results;
CREATE TABLE window_compat_results (r UInt64) ENGINE = Memory;

SET compatibility = '26.6';

INSERT INTO window_compat_results
SELECT groupBitXor(reinterpretAsUInt64(s))
FROM
(
    SELECT sum(multiIf(number % 3 = 0, 1e16, number % 3 = 1, -1e16, 1.)) OVER w AS s
    FROM numbers(20000)
    WINDOW w AS (ORDER BY number ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
)
SETTINGS max_block_size = 123;

SET compatibility = DEFAULT;

INSERT INTO window_compat_results
SELECT groupBitXor(reinterpretAsUInt64(s))
FROM
(
    SELECT sum(multiIf(number % 3 = 0, 1e16, number % 3 = 1, -1e16, 1.)) OVER w AS s
    FROM numbers(20000)
    WINDOW w AS (ORDER BY number ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
)
SETTINGS max_block_size = 123, min_window_frame_rows_for_aggregate_tree = 1000000000;

SELECT uniqExact(r) FROM window_compat_results;

DROP TABLE window_compat_results;
