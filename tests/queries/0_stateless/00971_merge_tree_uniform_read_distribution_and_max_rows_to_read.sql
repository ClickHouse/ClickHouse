DROP TABLE IF EXISTS merge_tree;
CREATE TABLE merge_tree (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO merge_tree SELECT 0 FROM numbers(1000000);

SET max_threads = 4;
SET max_rows_to_read = 1100000;

SET merge_tree_uniform_read_distribution = 1;
SELECT count() FROM merge_tree;

SET merge_tree_uniform_read_distribution = 0;
SELECT count() FROM merge_tree;

SET max_rows_to_read = 900000;

SET merge_tree_uniform_read_distribution = 1;
SELECT count() FROM merge_tree WHERE not ignore(); -- { serverError 158 }

SET merge_tree_uniform_read_distribution = 0;
SELECT count() FROM merge_tree WHERE not ignore(); -- { serverError 158 }

DROP TABLE merge_tree;
