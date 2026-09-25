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
-- under ASan. Ten copies of a 10x10 grid: the ten exact matches of the reference vector [5, 5] are the
-- whole result of a `LIMIT 10`, so deleting them used to empty it. Every row is its own granule, so
-- reading only the granules of the index candidates would read nothing but the deleted rows.

SET enable_analyzer = 1;
SET lightweight_deletes_sync = 2;
SET mutations_sync = 2;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_05205;
CREATE TABLE t_05205 (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO t_05205 SELECT number, [toFloat32(number % 10), toFloat32(intDiv(number, 10) % 10)] FROM numbers(1000);

-- The ten exact matches of the reference vector.
DELETE FROM t_05205 WHERE v = [5.0, 5.0];

SELECT 'rows left', count() FROM t_05205;
SELECT 'with the index', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10);
SELECT 'without the index', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS use_skip_indexes = 0);
SELECT 'without rescoring', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS vector_search_with_rescoring = 0);
SELECT 'with rescoring', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS vector_search_with_rescoring = 1);

-- And they are as near as the neighbours the bruteforce scan finds (forty rows tie at distance 1, so compare distances, not ids).
SELECT 'the index answer', arraySort(groupArray(d)) FROM (SELECT id, L2Distance(v, [5.0, 5.0]) AS d FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10);
SELECT 'the bruteforce answer', arraySort(groupArray(d)) FROM (SELECT id, L2Distance(v, [5.0, 5.0]) AS d FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS use_skip_indexes = 0);

-- Deleting a whole cluster of near neighbours used to empty the result as well.
DELETE FROM t_05205 WHERE L2Distance(v, [5.0, 5.0]) < 1.5;

SELECT 'a deleted cluster', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3);

-- A delete that is still a pending mutation, applied on the fly, has the same effect on the index.
DROP TABLE IF EXISTS t_05205_on_fly;
CREATE TABLE t_05205_on_fly (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO t_05205_on_fly SELECT number, [toFloat32(number % 10), toFloat32(intDiv(number, 10) % 10)] FROM numbers(1000);
SYSTEM STOP MERGES t_05205_on_fly;
ALTER TABLE t_05205_on_fly DELETE WHERE v = [5.0, 5.0] SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'a pending delete', count() FROM (SELECT id FROM t_05205_on_fly ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS apply_mutations_on_fly = 1);

SYSTEM START MERGES t_05205_on_fly;

-- A delete written as a patch part (a lightweight update of `_row_exists`) is invisible to the index too.
DROP TABLE IF EXISTS t_05205_patch;
CREATE TABLE t_05205_patch (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_05205_patch SELECT number, [toFloat32(number % 10), toFloat32(intDiv(number, 10) % 10)] FROM numbers(1000);
DELETE FROM t_05205_patch WHERE v = [5.0, 5.0] SETTINGS lightweight_delete_mode = 'lightweight_update';

SELECT 'a delete in a patch part', count() FROM (SELECT id FROM t_05205_patch ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS apply_patch_parts = 1);

DROP TABLE t_05205_patch;
DROP TABLE t_05205_on_fly;
DROP TABLE t_05205;
