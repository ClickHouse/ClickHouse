-- The PREWHERE optimizer accounts for the exact subcolumn read cost only when
-- `allow_calculating_subcolumns_sizes_for_merge_tree_reading` is enabled. When it is
-- disabled, the subcolumn is costed by its whole top-level column size instead.
--
-- The table has a Tuple where the `small` element is tiny and the `big` element is huge,
-- plus a `medium` column. With the setting on, `tup.small`'s exact size is smaller than
-- `medium`, so it goes first in PREWHERE. With the setting off, `tup.small` is costed by
-- the whole `tup` size (dominated by `big`), which is larger than `medium`, so `medium`
-- goes first. Both conditions use equality so both are "good"; statistics are disabled so
-- that only column sizes drive the ordering.
--
-- The analyzer path and the legacy InterpreterSelectQuery path are both covered; `tup.small`
-- stays a subcolumn in both.

SET optimize_move_to_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;
SET use_statistics = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prewhere_subcolumn_size;
CREATE TABLE t_prewhere_subcolumn_size (id UInt64, medium String, tup Tuple(small String, big String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_prewhere_subcolumn_size
SELECT number, repeat('m', 50), (repeat('s', 1), repeat('b', 500))
FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_subcolumn_size FINAL;

SET enable_analyzer = 1;
SET query_plan_optimize_prewhere = 1;

SELECT '-- analyzer path, setting on: exact subcolumn size, cheap tup.small first';
SELECT position(explain, 'tup.small') > 0 AND position(explain, 'tup.small') < position(explain, 'medium') AS subcolumn_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_subcolumn_size WHERE medium = 'x' AND tup.small = 'y'
) WHERE explain LIKE '%Prewhere filter column%'
SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;

SELECT '-- analyzer path, setting off: top-level column size, cheap medium first';
SELECT position(explain, 'tup.small') > 0 AND position(explain, 'tup.small') < position(explain, 'medium') AS subcolumn_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_subcolumn_size WHERE medium = 'x' AND tup.small = 'y'
) WHERE explain LIKE '%Prewhere filter column%'
SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;

SELECT '-- correctness: result is the same regardless of the setting';
SELECT count() FROM t_prewhere_subcolumn_size WHERE medium = 'x' AND tup.small = 'y'
SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;
SELECT count() FROM t_prewhere_subcolumn_size WHERE medium = 'x' AND tup.small = 'y'
SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;

SET enable_analyzer = 0;
SET query_plan_optimize_prewhere = 0;

SELECT '-- legacy path, setting on: exact subcolumn size, cheap tup.small first';
SELECT position(explain, 'tup.small') > 0 AND position(explain, 'tup.small') < position(explain, 'medium') AS subcolumn_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_subcolumn_size WHERE medium = 'x' AND tup.small = 'y'
) WHERE explain LIKE '%Prewhere filter column%'
SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;

SELECT '-- legacy path, setting off: top-level column size, cheap medium first';
SELECT position(explain, 'tup.small') > 0 AND position(explain, 'tup.small') < position(explain, 'medium') AS subcolumn_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_subcolumn_size WHERE medium = 'x' AND tup.small = 'y'
) WHERE explain LIKE '%Prewhere filter column%'
SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;

DROP TABLE t_prewhere_subcolumn_size;
