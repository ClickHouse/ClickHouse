DROP TABLE IF EXISTS merge_tree;
CREATE TABLE merge_tree (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO merge_tree SELECT 0 FROM numbers(1000000);

SET max_threads = 4;
SET max_rows_to_read = 1100000;

SELECT count() FROM merge_tree;
SELECT count() FROM merge_tree;

SET max_rows_to_read = 900000;

-- constant ignore will be pruned by part pruner. ignore(*) is used.
SELECT count() FROM merge_tree WHERE not ignore(*); -- { serverError TOO_MANY_ROWS }
SELECT count() FROM merge_tree WHERE not ignore(*); -- { serverError TOO_MANY_ROWS }

DROP TABLE merge_tree;
