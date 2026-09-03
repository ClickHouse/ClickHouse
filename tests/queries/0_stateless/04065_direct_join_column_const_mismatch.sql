-- Test that DirectJoinMergeTreeEntity handles ColumnConst columns correctly
-- when merging multiple blocks from the pipeline (convertToFullColumnIfConst).
--
-- We create two parts in t_right: the first part (written before ALTER) has no
-- stored new_col, so MergeTree fills it as ColumnConst on read. The second part
-- (written after ALTER) stores new_col as a regular column. With small
-- max_block_size, blocks from both parts are merged in executePlan, which
-- previously triggered an assertTypeEquality failure (debug/sanitizer builds)
-- or produced wrong results (release builds) in insertRangeFrom.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_right (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_right SELECT number, 'val_' || toString(number) FROM numbers(5);

-- Add a column after data is already written; the existing part does not
-- store it, so MergeTree fills it as ColumnConst on read.
ALTER TABLE t_right ADD COLUMN new_col String DEFAULT 'default_value';

-- Insert more data: this creates a second part where new_col IS stored with
-- actual (non-default) values, producing a regular column on read.
INSERT INTO t_right SELECT number + 5, 'val_' || toString(number + 5), 'stored_' || toString(number + 5) FROM numbers(5);

CREATE TABLE t_left (id UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_left SELECT number FROM numbers(10);

SET join_algorithm = 'direct';
SET max_block_size = 2;
SET enable_analyzer = 1;

SELECT l.id, r.value, r.new_col
FROM t_left AS l
INNER JOIN t_right AS r ON l.id = r.id
ORDER BY l.id;

SELECT '--';

SELECT l.id, r.value, r.new_col
FROM t_left AS l
LEFT JOIN t_right AS r ON l.id = r.id
ORDER BY l.id;

DROP TABLE t_left;
DROP TABLE t_right;

SELECT '--- sparse columns ---';

-- Test that ColumnSparse mixed with regular columns across blocks
-- does not trigger assertTypeEquality failure in executePlan.
-- With ratio_of_defaults_for_sparse_serialization = 0, some parts may
-- produce ColumnSparse on read while others produce regular columns.

CREATE TABLE t_right_sparse (id UInt64, value String, extra UInt64 DEFAULT 0)
ENGINE = MergeTree() ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0;

-- First part: extra column is all defaults → may read as ColumnSparse
INSERT INTO t_right_sparse SELECT number, 'val_' || toString(number), 0 FROM numbers(5);

-- Second part: extra column has non-default values → regular column
INSERT INTO t_right_sparse SELECT number + 5, 'val_' || toString(number + 5), number + 100 FROM numbers(5);

CREATE TABLE t_left_sparse (id UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_left_sparse SELECT number FROM numbers(10);

SET join_algorithm = 'direct';
SET max_block_size = 2;
SET enable_analyzer = 1;

SELECT l.id, r.value, r.extra
FROM t_left_sparse AS l
INNER JOIN t_right_sparse AS r ON l.id = r.id
ORDER BY l.id;

DROP TABLE t_left_sparse;
DROP TABLE t_right_sparse;
