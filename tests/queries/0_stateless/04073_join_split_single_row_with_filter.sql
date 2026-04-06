-- Test that joined_block_split_single_row works correctly with need_filter.
-- When need_filter is true, scattered_block->filter(matched_rows) reduces the block size,
-- making offsets stale. The joined_block_split_single_row optimization must be disabled
-- in this case to avoid size mismatches and incorrect results.

SET joined_block_split_single_row = 1;
SET max_joined_block_size_rows = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt32, attr UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt32, attr UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (0, 0), (1, 1), (2, 2);
INSERT INTO t2 VALUES (0, 10), (1, 11), (2, 12);

-- The extra condition in ON triggers need_filter (has_required_right_keys).
-- Without the fix, this would return empty results or throw an exception.
SELECT t1.id, t1.attr, t2.id, t2.attr
FROM t1 JOIN t2 ON t1.id = t2.id AND t1.attr != 0
ORDER BY t1.id;

DROP TABLE t1;
DROP TABLE t2;
