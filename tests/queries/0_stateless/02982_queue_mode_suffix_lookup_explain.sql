DROP TABLE IF EXISTS queue_mode_test;

CREATE TABLE queue_mode_test(a UInt64, b UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY a SETTINGS queue_mode=1, index_granularity=1;

INSERT INTO queue_mode_test (*) SELECT 0, number FROM numbers(5); -- _queue_block_number = 1
INSERT INTO queue_mode_test (*) SELECT 0, number FROM numbers(5); -- _queue_block_number = 2
INSERT INTO queue_mode_test (*) SELECT 0, number FROM numbers(5); -- _queue_block_number = 3
INSERT INTO queue_mode_test (*) SELECT 0, number FROM numbers(5); -- _queue_block_number = 4

INSERT INTO queue_mode_test (*) SELECT 1, number FROM numbers(5); -- _queue_block_number = 5
INSERT INTO queue_mode_test (*) SELECT 1, number FROM numbers(5); -- _queue_block_number = 6
INSERT INTO queue_mode_test (*) SELECT 1, number FROM numbers(5); -- _queue_block_number = 7
INSERT INTO queue_mode_test (*) SELECT 1, number FROM numbers(5); -- _queue_block_number = 8

OPTIMIZE TABLE queue_mode_test;

SELECT 'query-1';

-- [part-1](3 + 5) + [part-2](3 + 5 + 5 + 5) = 26

SELECT * FROM
(
    EXPLAIN indexes=1 SELECT * FROM queue_mode_test WHERE
    (_queue_partition_id = '0' AND (_queue_block_number > 3 OR (_queue_block_number = 3 AND _queue_block_offset > 2))) OR
    (_queue_partition_id = '1' AND (_queue_block_number > 5 OR (_queue_block_number = 5 AND _queue_block_offset > 2)))
)
WHERE explain LIKE '%Granules: %'
LIMIT 1 OFFSET 2;

SELECT 'query-2';

-- [part-1](3 + 5) + [part-2](3 + 5) = 16

SELECT * FROM
(
    EXPLAIN indexes=1 SELECT * FROM queue_mode_test WHERE
    (_queue_partition_id = '0' AND (_queue_block_number > 3 OR (_queue_block_number = 3 AND _queue_block_offset > 2))) OR
    (_queue_partition_id = '1' AND (_queue_block_number > 7 OR (_queue_block_number = 7 AND _queue_block_offset > 2)))
)
WHERE explain LIKE '%Granules: %'
LIMIT 1 OFFSET 2;
