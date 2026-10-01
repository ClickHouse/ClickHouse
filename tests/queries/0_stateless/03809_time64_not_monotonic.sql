-- Regression test for non-monotonic Time64 conversion in MergeTreeSetIndex.
-- Converting from DateTime to Time64 extracts time-of-day component, which is not monotonic.

DROP TABLE IF EXISTS mt_test;
DROP TABLE IF EXISTS merge_test;

CREATE TABLE mt_test (d Date DEFAULT toDate('2015-05-01'), x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 1000000;
INSERT INTO mt_test (x) SELECT number AS x FROM system.numbers LIMIT 100000;

-- Create a Merge table with Time64 column type
CREATE TABLE merge_test (d Date, x Time64(3)) ENGINE = Merge(currentDatabase(), '^mt_test$');

-- This query previously caused "Invalid binary search result in MergeTreeSetIndex" exception
-- because the conversion from UInt64 to Time64 was incorrectly marked as monotonic.
SELECT count() > 0 FROM merge_test WHERE x NOT IN (12345, 67890);

DROP TABLE merge_test;
DROP TABLE mt_test;
