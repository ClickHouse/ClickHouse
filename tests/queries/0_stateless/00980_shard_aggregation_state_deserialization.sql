-- Tags: shard

DROP TABLE IF EXISTS numbers500k;
CREATE VIEW numbers500k AS SELECT number FROM system.numbers LIMIT 500000;

SET max_query_size = 1073741824;

SELECT count(*) FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) WHERE bitmapContains((SELECT groupBitmapState(number) FROM numbers500k), toUInt32(number));

DROP TABLE numbers500k;
