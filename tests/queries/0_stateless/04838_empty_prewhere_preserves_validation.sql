DROP TABLE IF EXISTS empty_prewhere_preserves_validation;

CREATE TABLE empty_prewhere_preserves_validation
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
SAMPLE BY id;

INSERT INTO empty_prewhere_preserves_validation VALUES (1, 1);

SELECT count()
FROM empty_prewhere_preserves_validation SAMPLE 1 OFFSET 0.5
PREWHERE value IN (SELECT toUInt64(1) WHERE false); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT count()
FROM empty_prewhere_preserves_validation
PREWHERE (_partition_id = 'all') AND value IN (SELECT toUInt64(1) WHERE false)
SETTINGS
    enable_analyzer = 0,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 1,
    parallel_replicas_only_with_analyzer = 0;

DROP TABLE empty_prewhere_preserves_validation;
