-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-fasttest: the vector similarity index is not built in the fast test.
-- no-parallel-replicas: vector-search read hints are produced during local index analysis.

-- A lightweight `DELETE` does not rebuild a vector index, so the index keeps returning the row ids of
-- deleted rows. Restricting the read to exactly those rows then drops the deleted candidates with
-- nothing to take their place: the query returned fewer rows than its `LIMIT` and missed the true
-- neighbours that a deleted candidate shadowed, down to an empty result when a whole cluster of near
-- neighbours is deleted. Such a part is read in full, without the index, until a merge or a mutation
-- rebuilds the index.

-- The tables are small, with a small HNSW graph, because every index build runs for more than a minute
-- under ASan. Three copies of a 10x10 grid: the three exact matches of the reference vector [5, 5] are the
-- whole result of a `LIMIT 3`, so deleting them used to empty it. Every row is its own granule, so
-- reading only the granules of the index candidates would read nothing but the deleted rows.
-- Direct I/O is disabled because it reads every one-row granule separately and makes the test slow.

SET enable_analyzer = 1;
SET lightweight_deletes_sync = 2;
SET mutations_sync = 2;
SET parallel_replicas_local_plan = 1;
SET min_bytes_to_use_direct_io = 0;

DROP TABLE IF EXISTS t_05205;
-- `ReplacingMergeTree` (the ids are unique, so it reads like a `MergeTree`) to check the `FINAL` read as well.
CREATE TABLE t_05205 (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32))
ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO t_05205 SELECT number, [toFloat32(number % 10), toFloat32(intDiv(number, 10) % 10)] FROM numbers(300);

-- The three exact matches of the reference vector.
DELETE FROM t_05205 WHERE v = [5.0, 5.0];

SELECT 'rows left', count() FROM t_05205;
SELECT 'with the index', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3);
SELECT 'without the index', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS use_skip_indexes = 0);
SELECT 'without rescoring', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS vector_search_with_rescoring = 0);
SELECT 'with rescoring', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS vector_search_with_rescoring = 1);

-- And they are as near as the neighbours the bruteforce scan finds (twelve rows tie at distance 1, so compare distances, not ids).
SELECT 'the index answer', arraySort(groupArray(d)) FROM (SELECT id, L2Distance(v, [5.0, 5.0]) AS d FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3);
SELECT 'the bruteforce answer', arraySort(groupArray(d)) FROM (SELECT id, L2Distance(v, [5.0, 5.0]) AS d FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS use_skip_indexes = 0);

-- With `apply_deleted_mask = 0` the deleted rows are returned, so the index still matches the data and is used.
SELECT 'apply_deleted_mask = 0 uses the index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS apply_deleted_mask = 0, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '% Granules: 3/300';
SELECT 'apply_deleted_mask = 0', arraySort(groupArray(d)) FROM (SELECT L2Distance(v, [5.0, 5.0]) AS d FROM t_05205 ORDER BY d LIMIT 3 SETTINGS apply_deleted_mask = 0);
-- But an explicit filter by `_row_exists` drops them again, the same as the default masked read, so the index is not used.
SELECT 'apply_deleted_mask = 0 and _row_exists uses the index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_05205 WHERE _row_exists ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS apply_deleted_mask = 0, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '% Granules: 3/300';
SELECT 'apply_deleted_mask = 0 and _row_exists', count() FROM (SELECT id FROM t_05205 WHERE _row_exists ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS apply_deleted_mask = 0);
SELECT 'apply_deleted_mask = 0 and _row_exists', arraySort(groupArray(d)) FROM (SELECT L2Distance(v, [5.0, 5.0]) AS d FROM t_05205 WHERE _row_exists ORDER BY d LIMIT 3 SETTINGS apply_deleted_mask = 0);
-- The same with `FINAL`, where the `PREWHERE` on `_row_exists` is deferred until after the `FINAL` merge.
SELECT 'FINAL, apply_deleted_mask = 0 and deferred PREWHERE _row_exists uses the index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_05205 FINAL PREWHERE _row_exists ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS apply_deleted_mask = 0, apply_prewhere_after_final = 1, use_skip_indexes_on_data_read = 0) WHERE explain LIKE '% Granules: 3/300';
SELECT 'FINAL, apply_deleted_mask = 0 and deferred PREWHERE _row_exists', count() FROM (SELECT id FROM t_05205 FINAL PREWHERE _row_exists ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS apply_deleted_mask = 0, apply_prewhere_after_final = 1);

-- Deleting a whole cluster of near neighbours used to empty the result as well.
DELETE FROM t_05205 WHERE L2Distance(v, [5.0, 5.0]) < 1.5;

SELECT 'a deleted cluster', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3);

-- A delete that is still a pending mutation, applied on the fly, has the same effect on the index.
DROP TABLE IF EXISTS t_05205_on_fly;
CREATE TABLE t_05205_on_fly (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO t_05205_on_fly SELECT number, [toFloat32(number % 10), toFloat32(intDiv(number, 10) % 10)] FROM numbers(300);
SYSTEM STOP MERGES t_05205_on_fly;
ALTER TABLE t_05205_on_fly DELETE WHERE v = [5.0, 5.0] SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'a pending delete', count() FROM (SELECT id FROM t_05205_on_fly ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS apply_mutations_on_fly = 1);

SYSTEM START MERGES t_05205_on_fly;

-- A delete written as a patch part (a lightweight update of `_row_exists`) is invisible to the index too.
DROP TABLE IF EXISTS t_05205_patch;
CREATE TABLE t_05205_patch (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_05205_patch SELECT number, [toFloat32(number % 10), toFloat32(intDiv(number, 10) % 10)] FROM numbers(300);
DELETE FROM t_05205_patch WHERE v = [5.0, 5.0] SETTINGS lightweight_delete_mode = 'lightweight_update';

SELECT 'a delete in a patch part', count() FROM (SELECT id FROM t_05205_patch ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3 SETTINGS apply_patch_parts = 1);

DROP TABLE t_05205_patch;
DROP TABLE t_05205_on_fly;
DROP TABLE t_05205;
