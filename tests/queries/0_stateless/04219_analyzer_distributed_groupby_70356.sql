-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/70356
-- `NOT_FOUND_COLUMN_IN_BLOCK` on distributed table queries under the analyzer.

DROP TABLE IF EXISTS shard_table_70356;
DROP TABLE IF EXISTS dist_table_70356;
DROP TABLE IF EXISTS dist_two_shards_table_70356;

CREATE TABLE shard_table_70356
(
    adid FixedString(16),
    created_at_dt Date,
    event_name String
)
ENGINE = MergeTree
ORDER BY event_name;

INSERT INTO shard_table_70356 VALUES ('0123456789abcdef', '2024-01-01', 'install');
INSERT INTO shard_table_70356 VALUES ('fedcba9876543210', '2024-01-02', 'purchase');

CREATE TABLE dist_table_70356 AS shard_table_70356
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'shard_table_70356', sipHash64(adid));

CREATE TABLE dist_two_shards_table_70356 AS shard_table_70356
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 'shard_table_70356', sipHash64(adid));

-- Original failing query shape from the issue: no `ORDER BY`, `FORMAT Null`.
-- Kept as-is to guard the exact reported regression.
SELECT tt.adid AS id
FROM dist_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
SETTINGS enable_analyzer = 1, distributed_product_mode = 'global'
FORMAT Null;

SELECT tt.adid AS id
FROM dist_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
SETTINGS enable_analyzer = 1
FORMAT Null;

SELECT tt.adid AS id
FROM dist_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
ORDER BY id
SETTINGS enable_analyzer = 1, distributed_product_mode = 'global';

SELECT tt.adid AS id
FROM dist_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
ORDER BY id
SETTINGS enable_analyzer = 1;

-- Two-shard variants in the original failing shape (`FORMAT Null`, no `ORDER BY`).
SELECT tt.adid AS id
FROM dist_two_shards_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
SETTINGS enable_analyzer = 1, distributed_product_mode = 'global'
FORMAT Null;

SELECT tt.adid AS id
FROM dist_two_shards_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
SETTINGS enable_analyzer = 1
FORMAT Null;

-- Both shards of `test_cluster_two_shards_localhost` resolve to the same
-- backing table, so each shard returns the same row. Pin
-- `optimize_distributed_group_by_sharding_key` to keep the coordinator-side
-- merge stable regardless of randomized session settings.
SELECT tt.adid AS id
FROM dist_two_shards_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
ORDER BY id
SETTINGS enable_analyzer = 1, distributed_product_mode = 'global', optimize_distributed_group_by_sharding_key = 0;

SELECT tt.adid AS id
FROM dist_two_shards_table_70356 AS tt
WHERE tt.created_at_dt = '2024-01-01'
GROUP BY 1
ORDER BY id
SETTINGS enable_analyzer = 1, optimize_distributed_group_by_sharding_key = 0;

DROP TABLE dist_two_shards_table_70356;
DROP TABLE dist_table_70356;
DROP TABLE shard_table_70356;
