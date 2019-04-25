DROP TABLE IF EXISTS merge_tree;
CREATE TABLE merge_tree (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO merge_tree VALUES (0), (1);

SET force_primary_key = 1;
SET max_rows_to_read = 1;

SELECT count() FROM merge_tree WHERE x = 0;
SELECT count() FROM merge_tree WHERE toUInt32(x) = 0;
SELECT count() FROM merge_tree WHERE toUInt64(x) = 0;

SELECT count() FROM merge_tree WHERE x IN (0, 0);
SELECT count() FROM merge_tree WHERE toUInt32(x) IN (0, 0);
SELECT count() FROM merge_tree WHERE toUInt64(x) IN (0, 0);

DROP TABLE merge_tree;
