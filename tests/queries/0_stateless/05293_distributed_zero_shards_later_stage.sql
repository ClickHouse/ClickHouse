-- Tags: distributed

-- A `Distributed` read whose shards are all skipped by `optimize_skip_unused_shards` returns an empty result
-- with `distributed_group_by_no_merge`, and the empty-set aggregate when it is read by another `Distributed` table.

DROP TABLE IF EXISTS zero_shards_dist_over_dist;
DROP TABLE IF EXISTS zero_shards_dist;
DROP TABLE IF EXISTS zero_shards_local;

CREATE TABLE zero_shards_local
(
    dt DateTime,
    x UInt8,
    a1 String ALIAS toString(x),
    a2 String ALIAS toString(x),
    b UInt8 ALIAS x + 1
)
ENGINE = MergeTree ORDER BY dt;

CREATE TABLE zero_shards_dist
(
    dt DateTime,
    x UInt8,
    a1 String ALIAS toString(x),
    a2 String ALIAS toString(x),
    b UInt8 ALIAS x + 1
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), zero_shards_local, x);

-- `b` is an ordinary column here, so the inner read resolves the ALIAS column of `zero_shards_dist`.
CREATE TABLE zero_shards_dist_over_dist (dt DateTime, x UInt8, b UInt8)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), zero_shards_dist);

INSERT INTO zero_shards_local (dt, x) VALUES ('2024-01-01 00:00:00', 7);

SET optimize_skip_unused_shards = 1;

-- { echoOn }
SELECT b FROM zero_shards_dist WHERE 0 SETTINGS distributed_group_by_no_merge = 1;
SELECT b FROM zero_shards_dist WHERE x = 1 AND x = 2 SETTINGS distributed_group_by_no_merge = 1;
SELECT b FROM zero_shards_dist WHERE 0 SETTINGS distributed_group_by_no_merge = 2;
SELECT x, x + 1 FROM zero_shards_dist WHERE 0 SETTINGS distributed_group_by_no_merge = 1;
SELECT a1, a2, a1 FROM zero_shards_dist WHERE 0 ORDER BY dt DESC LIMIT 1 SETTINGS distributed_group_by_no_merge = 1;
SELECT count(), sum(x) FROM zero_shards_dist_over_dist WHERE 0;
SELECT sum(b) FROM zero_shards_dist_over_dist WHERE 0;
SELECT count(), sum(x) FROM zero_shards_dist_over_dist WHERE 0 SETTINGS optimize_skip_unused_shards = 0;
SELECT b FROM zero_shards_dist WHERE x = 7 SETTINGS distributed_group_by_no_merge = 1;
SELECT count(), sum(b) FROM zero_shards_dist WHERE 0;
-- { echoOff }

DROP TABLE zero_shards_dist_over_dist;
DROP TABLE zero_shards_dist;
DROP TABLE zero_shards_local;
