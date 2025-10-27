-- Test bitmap index granule filtering effectiveness
DROP TABLE IF EXISTS test_bitmap_granules;

-- Create table with bitmap index and small granularity to test filtering
CREATE TABLE test_bitmap_granules
(
    id     UInt32,
    status String,
    INDEX status_idx status TYPE bitmap(1000) GRANULARITY 1
)
    ENGINE = MergeTree()
        ORDER BY id
        PRIMARY KEY id;

INSERT INTO test_bitmap_granules
SELECT number, if(number < 10, 'active', 'inactive')
FROM numbers(100000);

SELECT count()
FROM test_bitmap_granules
WHERE status = 'inactive';

SELECT count()
FROM test_bitmap_granules
WHERE status = 'active';

SELECT count()
FROM test_bitmap_granules
WHERE status = 'pending';

SELECT count()
FROM test_bitmap_granules
WHERE status IN ('active', 'pending');

DROP TABLE test_bitmap_granules;

