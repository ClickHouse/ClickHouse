DROP TABLE IF EXISTS default.test_pushdown_limit_to_shards;
DROP TABLE IF EXISTS default.test_pushdown_limit_to_shards_dist;

CREATE TABLE default.test_pushdown_limit_to_shards (d Date, no Int32, name String, duration Nullable(Int32)) ENGINE = MergeTree PARTITION BY d ORDER BY no;
CREATE TABLE default.test_pushdown_limit_to_shards_dist (d Date, no Int32, name String, duration Nullable(Int32)) ENGINE = Distributed(test_cluster_two_shards_localhost, default, test_pushdown_limit_to_shards);

INSERT INTO default.test_pushdown_limit_to_shards VALUES (now(), 1, 'd', NULL) (now(), 2, 'd', NULL) (now(), 3, 'd', NULL) (now(), 4, 'd', NULL) (now(), 1, 'b', 1) (now(), 2, 'b', 1) (now(), 1, 'e', NULL) (now(), 2, 'e', NULL) (now(), 3, 'e', NULL);

SELECT name, count() AS c FROM default.test_pushdown_limit_to_shards_dist GROUP BY name ORDER BY c DESC LIMIT 2 SETTINGS experimental_enable_pushdown_limit_to_shards=1, limit_pushdown_fetch_multiplier=2;

WITH ifNull(sum(duration), 99999) AS tot SELECT name, tot, uniq(no) AS u FROM default.test_pushdown_limit_to_shards_dist GROUP BY name ORDER BY tot LIMIT 2 SETTINGS experimental_enable_pushdown_limit_to_shards=1;
WITH ifNull(sum(duration), 99999) AS tot SELECT name, tot, uniq(no) AS u FROM default.test_pushdown_limit_to_shards_dist GROUP BY name ORDER BY tot DESC LIMIT 2 SETTINGS experimental_enable_pushdown_limit_to_shards=1;

DROP TABLE IF EXISTS default.test_pushdown_limit_to_shards_dist;
DROP TABLE IF EXISTS default.test_pushdown_limit_to_shards;
