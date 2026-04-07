DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS dt;

CREATE TABLE t (tag_id UInt64, tag_name String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dt AS t ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 't', cityHash64(concat(tag_id, tag_name)));

INSERT INTO dt SETTINGS distributed_foreground_insert=1 VALUES (1, 'foo1'); -- shard0
INSERT INTO dt SETTINGS distributed_foreground_insert=1 VALUES (1, 'foo2'); -- shard1

SET optimize_skip_unused_shards=1, optimize_skip_unused_shards_rewrite_in=1;
-- { echoOn }
SELECT shardNum(), count() FROM dt WHERE (tag_id, tag_name) IN ((1, 'foo1'), (1, 'foo2')) GROUP BY 1 ORDER BY 1;
SELECT shardNum(), count() FROM dt WHERE tag_id IN (1, 1) AND tag_name IN ('foo1', 'foo2') GROUP BY 1 ORDER BY 1;
SELECT shardNum(), count() FROM dt WHERE tag_id = 1 AND tag_name IN ('foo1', 'foo2') GROUP BY 1 ORDER BY 1;
