-- Tags: no-fasttest, no-ordinary-database, no-replicated-database
-- Tag no-fasttest: the storage policy `local_remote` needs S3
-- Tag no-replicated-database: `ALTER TABLE ... MOVE PARTITION ... TO VOLUME` is not replicated

-- With `transfer_caches_on_disk_move`, a part with a vector similarity index that is moved to
-- another disk keeps its entries in the vector similarity index cache: the first vector search
-- after the move finds them under the keys of the moved part. Without it, that search loads the
-- index granule again from the new disk.

SET alter_move_to_space_execute_async = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_enabled;
DROP TABLE IF EXISTS t_disabled;

CREATE TABLE t_enabled (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'local_remote', index_granularity = 100, transfer_caches_on_disk_move = 1;

CREATE TABLE t_disabled (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'local_remote', index_granularity = 100, transfer_caches_on_disk_move = 0;

INSERT INTO t_enabled SELECT number, [toFloat32(number), toFloat32(0.0)] FROM numbers(1000);
INSERT INTO t_disabled SELECT number, [toFloat32(number), toFloat32(0.0)] FROM numbers(1000);

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec) FROM t_enabled
ORDER BY L2Distance(vec, reference_vec) LIMIT 1 SETTINGS log_comment = '05370:enabled_warm';

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec) FROM t_disabled
ORDER BY L2Distance(vec, reference_vec) LIMIT 1 SETTINGS log_comment = '05370:disabled_warm';

ALTER TABLE t_enabled MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_disabled MOVE PARTITION tuple() TO VOLUME 'remote';

SELECT table, disk_name FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_enabled', 't_disabled') AND active ORDER BY table;

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec) FROM t_enabled
ORDER BY L2Distance(vec, reference_vec) LIMIT 1 SETTINGS log_comment = '05370:enabled_to_remote';

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec) FROM t_disabled
ORDER BY L2Distance(vec, reference_vec) LIMIT 1 SETTINGS log_comment = '05370:disabled_to_remote';

-- And back to the local disk.

ALTER TABLE t_enabled MOVE PARTITION tuple() TO VOLUME 'local';

SELECT disk_name FROM system.parts
WHERE database = currentDatabase() AND table = 't_enabled' AND active;

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec) FROM t_enabled
ORDER BY L2Distance(vec, reference_vec) LIMIT 1 SETTINGS log_comment = '05370:enabled_to_local';

SYSTEM FLUSH LOGS query_log;

-- The vector searches after the moves: only the ones on the table with the transfer disabled have
-- to load the index granule again.
SELECT
    replaceOne(log_comment, '05370:', '') AS search,
    ProfileEvents['VectorSimilarityIndexCacheHits'] AS hits,
    ProfileEvents['VectorSimilarityIndexCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment LIKE '05370:%'
ORDER BY search;

DROP TABLE t_enabled;
DROP TABLE t_disabled;
