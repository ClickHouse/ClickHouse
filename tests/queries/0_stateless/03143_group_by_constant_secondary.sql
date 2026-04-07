-- https://github.com/ClickHouse/ClickHouse/issues/63264
SELECT count()
FROM remote(test_cluster_two_shards, system, one)
GROUP BY 'hi'
SETTINGS
    enable_analyzer = 1,
    group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 33950592;
