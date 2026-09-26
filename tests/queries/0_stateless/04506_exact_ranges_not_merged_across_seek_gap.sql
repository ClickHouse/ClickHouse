-- The generic exclusion search must not merge exact ranges across a gap of up to
-- `merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek` marks: the gap marks do not fully
-- match the condition, but the exact-count optimization counts all rows of exact ranges without
-- reading them, so absorbing gap marks produces a wrong `count()` result.

DROP TABLE IF EXISTS t_exact_ranges_seek;

CREATE TABLE t_exact_ranges_seek (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 1;

INSERT INTO t_exact_ranges_seek SELECT 1, number FROM numbers(100);

-- The exact-count optimization is what consumes the exact ranges, so it must be enabled for the
-- queries below to exercise them.
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

-- { echoOn }

-- The filter matches two runs of granules separated by a gap of non-matching granules. Filtering on
-- `b` (a non-prefix primary key column) forces the generic exclusion search.
SELECT count() FROM t_exact_ranges_seek WHERE (b BETWEEN 10 AND 13) OR (b BETWEEN 20 AND 23);

-- With a seek threshold larger than the gap, the ranges to read are merged into one, but the exact
-- ranges must stay separate.
SELECT count() FROM t_exact_ranges_seek WHERE (b BETWEEN 10 AND 13) OR (b BETWEEN 20 AND 23)
SETTINGS merge_tree_min_rows_for_seek = 7;

SELECT count() FROM t_exact_ranges_seek WHERE (b BETWEEN 10 AND 13) OR (b BETWEEN 20 AND 23)
SETTINGS merge_tree_min_bytes_for_seek = 1000000000;

-- Control: without the exact-count optimization the rows are read and filtered, so the result is
-- correct regardless of the seek settings.
SELECT count() FROM t_exact_ranges_seek WHERE (b BETWEEN 10 AND 13) OR (b BETWEEN 20 AND 23)
SETTINGS merge_tree_min_rows_for_seek = 7, optimize_use_projections = 0;

-- { echoOff }

DROP TABLE t_exact_ranges_seek;
