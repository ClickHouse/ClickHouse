-- Tags: no-parallel-replicas
DROP TABLE IF EXISTS tck;
CREATE TABLE tck (x Nullable(UInt32), v UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tck VALUES (1, 10), (2, 20), (NULL, 30), (4, 40);

SELECT count() FROM tck SETTINGS max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1,
    parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'tck.x',
    parallel_replicas_count = 2, parallel_replica_offset = 0, parallel_replicas_for_non_replicated_merge_tree = 0; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

SELECT count() FROM tck SETTINGS max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1,
    parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'CAST(tck.x AS UInt32)', cast_keep_nullable = 1,
    parallel_replicas_count = 2, parallel_replica_offset = 1, parallel_replicas_for_non_replicated_merge_tree = 0; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE tck;
