Set aggregation_max_size_for_arena_chunk=1500000000;
DROP TABLE IF EXISTS test.test_bitmap;

CREATE TABLE test.test_bitmap (`dimension` String, `bitmap` AggregateFunction(groupBitmap, UInt32) , `pt` Date) ENGINE = MergeTree PARTITION BY toYYYYMMDD(pt) ORDER BY dimension SETTINGS index_granularity = 128;

INSERT INTO test.test_bitmap SELECT 'test1',groupBitmapState(toUInt32(number)),'2019-09-01' FROM (SELECT number FROM system.numbers WHERE number>0 limit 100000000);

INSERT INTO test.test_bitmap SELECT 'test2',groupBitmapState(toUInt32(number)),'2019-09-01' FROM (SELECT number FROM system.numbers WHERE number>0 limit 100);

SELECT bitmapCardinality(bitmapAnd(b[1], b[2])) FROM (SELECT groupArray(bitmap) AS b FROM test.test_bitmap WHERE (dimension = 'test1') OR (dimension = 'test2'));

DROP TABLE IF EXISTS test.test_bitmap;
