-- Issue: https://github.com/ClickHouse/ClickHouse/issues/15995

DROP TABLE IF EXISTS outer;
DROP TABLE IF EXISTS inner;

DROP TABLE IF EXISTS outer_distributed;
DROP TABLE IF EXISTS inner_distributed;

CREATE TABLE IF NOT EXISTS outer
(
    `id` UInt64,
    `organization_id` UInt64,
    `version` UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY organization_id % 8
ORDER BY (organization_id, id);

CREATE TABLE inner
(
    `id` UInt64,
    `outer_id` UInt64,
    `organization_id` UInt64,
    `version` UInt64,
    `date` Date
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(date)
ORDER BY (organization_id, outer_id);

CREATE TABLE inner_distributed AS inner
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 'inner', intHash64(organization_id));

CREATE TABLE outer_distributed AS outer
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 'outer', intHash64(organization_id));

SELECT
    sum(if(inner_distributed.id != 0, 1, 0)) AS total,
    inner_distributed.date AS date
FROM outer_distributed AS outer_distributed
FINAL
LEFT JOIN
(
    SELECT
        inner_distributed.outer_id AS outer_id,
        inner_distributed.id AS id,
        inner_distributed.date AS date
    FROM inner_distributed AS inner_distributed
    FINAL
    WHERE inner_distributed.organization_id = 15078
) AS inner_distributed ON inner_distributed.outer_id = outer_distributed.id
WHERE (outer_distributed.organization_id = 15078) AND (date != toDate('1970-01-01'))
GROUP BY date
ORDER BY date DESC
SETTINGS distributed_product_mode = 'local', optimize_skip_unused_shards = 1;
