-- Tags: shard

DROP TABLE IF EXISTS x;
DROP TABLE IF EXISTS x_dist;

CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number;

CREATE TABLE x_dist as x ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), x, rand());

INSERT INTO x_dist SELECT * FROM numbers(10);

SET optimize_rewrite_sum_if_to_count_if = 1;

SELECT sumIf(1, number % 2 > 2) FROM x_dist FORMAT Null;

DROP TABLE IF EXISTS x;
DROP TABLE IF EXISTS x_dist;
