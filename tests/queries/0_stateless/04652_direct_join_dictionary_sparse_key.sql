-- Tags: no-parallel-replicas
-- DirectKeyValueJoin (the algorithm under test) cannot be chosen with parallel replicas, so the
-- ParallelReplicas runner variant would throw NOT_IMPLEMENTED instead of exercising the fix.
-- getColumnVectorData returned a reference into a column owned only by a function-local ColumnPtr
-- whenever the key column had to be materialized, so a Sparse-serialized left join key read freed
-- memory in FlatDictionary::hasKeys / ::getColumn.

DROP DICTIONARY IF EXISTS dict_sparse_key;
DROP TABLE IF EXISTS probe_sparse;
DROP TABLE IF EXISTS probe_dense;
DROP TABLE IF EXISTS probe_sparse_arr;
DROP TABLE IF EXISTS dict_source;

CREATE TABLE dict_source (j UInt64, v UInt64) ENGINE = MergeTree ORDER BY j;
INSERT INTO dict_source SELECT number, number * 10 FROM numbers(20);

CREATE DICTIONARY dict_sparse_key (j UInt64, v UInt64) PRIMARY KEY j
SOURCE(CLICKHOUSE(TABLE 'dict_source' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

-- `j` is default-heavy, so it is serialized Sparse and reaches the dictionary lookup unmaterialized.
CREATE TABLE probe_sparse (k UInt32, j UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO probe_sparse SELECT number % 50, number % 20 FROM numbers(4000);

-- Same data written densely: the reference results must agree with the sparse table.
CREATE TABLE probe_dense (k UInt32, j UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO probe_dense SELECT number % 50, number % 20 FROM numbers(4000);

-- `j` is carried (not array-joined) through ARRAY JOIN, so with lazy replication it reaches the
-- lookup as a ColumnReplicated wrapping the Sparse column.
CREATE TABLE probe_sparse_arr (j UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO probe_sparse_arr SELECT number % 20, [1, 2, 3] FROM numbers(1500);

SELECT 'The join key is really serialized Sparse';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'probe_sparse' AND column = 'j' AND active;

SET join_algorithm = 'direct';

-- FlatDictionary::hasKeys is the site that read the freed buffer.
SELECT 'Sparse key, key presence only';
SELECT count(), countIf(r.j = 0) FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j;

-- FlatDictionary::getColumn is a second site reached through the same getByKeys call.
SELECT 'Sparse key, dictionary attribute read';
SELECT sum(r.v), count() FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j;

-- Aggregating in order over the sparse key: the shape observed failing in CI.
SELECT 'Sparse key, aggregation in order';
SELECT max(u), min(u), count() FROM
(
    SELECT l.k, uniqExact(l.k) AS u FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j GROUP BY l.k
)
SETTINGS optimize_aggregation_in_order = 1, max_threads = 1;

SELECT 'Sparse key replicated by ARRAY JOIN, attribute read';
SELECT sum(r.v), count() FROM (SELECT j FROM probe_sparse_arr ARRAY JOIN arr) AS l
LEFT JOIN dict_sparse_key AS r ON l.j = r.j
SETTINGS enable_lazy_columns_replication = 1;

-- The values above must equal what a dense key and a non-direct join produce on the same data.
SELECT 'Sparse result equals dense result';
SELECT
    (SELECT sum(r.v) FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j)
        = (SELECT sum(r.v) FROM probe_dense AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j);

SELECT 'Sparse result equals hash join result';
SELECT
    (SELECT sum(r.v) FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j)
        = (SELECT sum(r.v) FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j
           SETTINGS join_algorithm = 'hash');

SELECT 'Direct join is still chosen for the sparse key';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j
)
WHERE explain ILIKE '%Algorithm: DirectKeyValueJoin%';

DROP DICTIONARY dict_sparse_key;
DROP TABLE probe_sparse;
DROP TABLE probe_dense;
DROP TABLE probe_sparse_arr;
DROP TABLE dict_source;
