-- Test lightweight UPDATE of multiple Array subcolumns of one Nested column:
-- validateNestedArraySizes must still run after the patch-part prefilter.

DROP TABLE IF EXISTS t_lwu_multi_nested;
SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_multi_nested;

CREATE TABLE t_lwu_multi_nested (id UInt64, n Nested(a UInt64, b String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_multi_nested VALUES (1, [1, 2], ['x', 'y']);

-- Mismatched sizes across the two updated subcolumns must be rejected.
UPDATE t_lwu_multi_nested SET `n.a` = [10, 20], `n.b` = ['p'] WHERE id = 1; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Matching sizes succeed and both subcolumns are updated consistently.
UPDATE t_lwu_multi_nested SET `n.a` = [10, 20, 30], `n.b` = ['p', 'q', 'r'] WHERE id = 1;

SELECT `n.a`, `n.b`, length(`n.a`) = length(`n.b`) FROM t_lwu_multi_nested ORDER BY id;

DROP TABLE t_lwu_multi_nested;
