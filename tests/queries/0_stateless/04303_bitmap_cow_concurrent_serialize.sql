-- Verify that concurrent serialization of COW-enabled bitmap aggregate states
-- does not trigger a data race (validated by TSAN CI build).
-- The JOIN replicates the aggregate state pointer via ColumnAggregateFunction::replicate,
-- then multiple threads serialize the same shared state concurrently.

DROP TABLE IF EXISTS test_bitmap_cow_concurrent;

CREATE TABLE test_bitmap_cow_concurrent
(
    id UInt32,
    bm AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO test_bitmap_cow_concurrent
    SELECT 1, groupBitmapState(toUInt32(number)) FROM numbers(500000);

INSERT INTO test_bitmap_cow_concurrent
    SELECT 2, groupBitmapState(toUInt32(number + 250000)) FROM numbers(500000);

-- Concurrent merge + serialize: GROUP BY forces merge of replicated states across threads
SELECT id, groupBitmapMerge(bm) FROM test_bitmap_cow_concurrent
    CROSS JOIN numbers(4) AS n
    GROUP BY id
    ORDER BY id
    SETTINGS max_threads = 4;

-- Concurrent write path: INSERT INTO MergeTree serializes the aggregate state
-- while the source is shared via replicate from the CROSS JOIN expansion
INSERT INTO test_bitmap_cow_concurrent
    SELECT id, groupBitmapMergeState(bm) FROM test_bitmap_cow_concurrent
    CROSS JOIN numbers(4) AS n
    GROUP BY id
    SETTINGS max_threads = 4;

SELECT id, groupBitmapMerge(bm) FROM test_bitmap_cow_concurrent
    GROUP BY id
    ORDER BY id;

DROP TABLE test_bitmap_cow_concurrent;
