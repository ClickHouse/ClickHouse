-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-fasttest: the vector similarity index is not built in the fast test.
-- no-parallel-replicas: vector-search read hints are produced during local index analysis.

-- A lightweight `DELETE` does not rebuild a vector index, so the index keeps returning the row ids of
-- deleted rows. Restricting the read to exactly those rows then drops the deleted candidates with
-- nothing to take their place: the query returned fewer rows than its `LIMIT` and missed the true
-- neighbours that a deleted candidate shadowed, down to an empty result when a whole cluster of near
-- neighbours is deleted. The read falls back to the candidates' granules until a merge or a mutation
-- rebuilds the index.

SET enable_analyzer = 1;
SET lightweight_deletes_sync = 2;
SET mutations_sync = 2;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_05205;
CREATE TABLE t_05205 (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05205 SELECT number, [toFloat32(number % 97), toFloat32(number % 31)] FROM numbers(10000);

-- The four exact matches of the reference vector.
DELETE FROM t_05205 WHERE id IN (5, 3012, 6019, 9026);

SELECT 'rows left', count() FROM t_05205;
SELECT 'with the index', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10);
SELECT 'without the index', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS use_skip_indexes = 0);
SELECT 'without rescoring', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS vector_search_with_rescoring = 0);
SELECT 'with rescoring', count() FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS vector_search_with_rescoring = 1);

-- And they are the same neighbours the bruteforce scan finds.
SELECT 'the index answer', groupArray(id) FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]), id LIMIT 10);
SELECT 'the bruteforce answer', groupArray(id) FROM (SELECT id FROM t_05205 ORDER BY L2Distance(v, [5.0, 5.0]), id LIMIT 10 SETTINGS use_skip_indexes = 0);

-- Deleting a whole cluster of near neighbours used to empty the result.
DROP TABLE IF EXISTS t_05205_cluster;
CREATE TABLE t_05205_cluster (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05205_cluster SELECT number, [toFloat32(number % 97), toFloat32(number % 31)] FROM numbers(10000);
DELETE FROM t_05205_cluster WHERE L2Distance(v, [5.0, 5.0]) < 3;

SELECT 'a deleted cluster', count() FROM (SELECT id FROM t_05205_cluster ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 3);

-- A delete that is still a pending mutation, applied on the fly, has the same effect on the index.
DROP TABLE IF EXISTS t_05205_on_fly;
CREATE TABLE t_05205_on_fly (id UInt64, v Array(Float32), INDEX vidx v TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05205_on_fly SELECT number, [toFloat32(number % 97), toFloat32(number % 31)] FROM numbers(10000);
SYSTEM STOP MERGES t_05205_on_fly;
ALTER TABLE t_05205_on_fly DELETE WHERE id IN (5, 3012, 6019, 9026) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'a pending delete', count() FROM (SELECT id FROM t_05205_on_fly ORDER BY L2Distance(v, [5.0, 5.0]) LIMIT 10 SETTINGS apply_mutations_on_fly = 1);

SYSTEM START MERGES t_05205_on_fly;

DROP TABLE t_05205_on_fly;
DROP TABLE t_05205_cluster;
DROP TABLE t_05205;
