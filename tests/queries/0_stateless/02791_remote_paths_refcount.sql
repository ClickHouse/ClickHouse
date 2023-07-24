-- Tags: no-fasttest

DROP TABLE IF EXISTS t_refcount SYNC;

CREATE TABLE t_refcount (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/test/{database}/t_refcount', '1')
ORDER BY id PARTITION BY id % 2
SETTINGS
    storage_policy = 's3_cache',
    allow_remote_fs_zero_copy_replication = 1,
    min_bytes_for_wide_part = 0,
    compress_marks = 1,
    compress_primary_key = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_refcount VALUES (1, 10), (2, 20);

SET mutations_sync = 2;
ALTER TABLE t_refcount UPDATE v = v * 10 WHERE id % 2 = 1;

SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 't_refcount' ORDER BY name;

WITH splitByChar('/', full_path) AS path_parts
SELECT path_parts[-2] AS part_name, path_parts[-1] AS file_name, refcount
FROM
(
    SELECT
        path || local_path AS full_path,
        substring(full_path, 1, length(full_path) - position(reverse(full_path), '/') + 1) AS part_path,
        refcount
    FROM system.remote_data_paths
    WHERE disk_name = 's3_cache'
) AS paths
INNER JOIN
(
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_refcount' AND active
) AS parts
ON paths.part_path = parts.path
ORDER BY part_name, file_name;

DROP TABLE IF EXISTS t_refcount SYNC;
