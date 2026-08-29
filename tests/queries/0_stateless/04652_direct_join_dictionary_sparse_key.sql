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

-- The attribute is never 0 and join_use_nulls is pinned to 0 below, so `r.v = 0` in the results
-- below unambiguously means "key not found": that is what a lookup blind to the sparse default
-- key 0 would produce.
CREATE TABLE dict_source (j UInt64, v UInt64) ENGINE = MergeTree ORDER BY j;
INSERT INTO dict_source SELECT number, (number + 1) * 10 FROM numbers(20);

CREATE DICTIONARY dict_sparse_key (j UInt64, v UInt64) PRIMARY KEY j
SOURCE(CLICKHOUSE(TABLE 'dict_source' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

-- `j` is default-heavy, so it is serialized Sparse and reaches the dictionary lookup unmaterialized.
-- 25 keys x 160 rows: keys 0..19 are present in the dictionary, keys 20..24 are absent.
CREATE TABLE probe_sparse (k UInt32, j UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO probe_sparse SELECT number % 50, number % 25 FROM numbers(4000);

-- Same data written densely: the reference results must agree with the sparse table.
CREATE TABLE probe_dense (k UInt32, j UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO probe_dense SELECT number % 50, number % 25 FROM numbers(4000);

-- `j` is carried (not array-joined) through ARRAY JOIN over a Sparse base column, so it reaches the
-- lookup as a column that has to be materialized. The serialization guard below is what pins the
-- sparse base; the query does not distinguish lazy from eager replication.
CREATE TABLE probe_sparse_arr (j UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO probe_sparse_arr SELECT number % 25, [1, 2, 3] FROM numbers(1500);

SELECT 'The join keys are really serialized Sparse';
SELECT table, countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('probe_sparse', 'probe_sparse_arr')
  AND column = 'j' AND active
GROUP BY table ORDER BY table;

SET join_algorithm = 'direct';
-- The assertions below distinguish a dictionary hit from a miss by the attribute value, so the
-- representation of a miss must be fixed: with join_use_nulls = 1 a miss would be NULL instead of
-- the default 0. The Stress check injects join_use_nulls = 1 on some threads.
SET join_use_nulls = 0;

-- FlatDictionary::hasKeys is the site that read the freed buffer. Its result is the presence mask,
-- which getByKeys applies to the returned right KEY column only, so r.j is what observes the mask
-- directly: the attribute counts would not, because attributes are fetched independently of it. The
-- two counts below catch a misclassified key whose value is not 0; key 0 needs the nullable-key
-- query further down, because a blanked right key is 0 as well.
SELECT 'Sparse key, key presence only';
SELECT count(), countIf(r.v != 0), countIf(r.v = 0), countIf(l.j = 0 AND r.v = 10),
       countIf(l.j < 20 AND r.j = l.j), countIf(l.j >= 20 AND r.j = 0)
FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j;

-- FlatDictionary::getColumn is a second site reached through the same getByKeys call.
-- countIf(r.v = (l.j + 1) * 10) is the dictionary contents restated inline: it equals the number of
-- present-key rows only if every one of them carries the value belonging to its own key, so a
-- lookup that permutes values between keys is caught even though it preserves sum(r.v).
SELECT 'Sparse key, dictionary attribute read';
SELECT sum(r.v), count(), countIf(r.v = (l.j + 1) * 10), countIf(r.v = 0 AND l.j >= 20)
FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j;

-- Key 0 is the sparse default, and a blanked right key is also 0, so the assertion above cannot
-- tell a found key 0 from a lost one. Under join_use_nulls = 1 an unmatched row yields NULL for the
-- right key, which separates the two. This statement asks for that value explicitly, so the setting
-- the Stress check injects cannot change what it measures.
SELECT 'Sparse default key 0 is really found';
SELECT countIf(r.j IS NULL), countIf(l.j < 20 AND r.j IS NOT NULL), countIf(l.j = 0 AND r.j = 0)
FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j
SETTINGS join_use_nulls = 1;

-- Aggregating in order over the sparse key: the shape observed failing in CI.
SELECT 'Sparse key, aggregation in order';
SELECT max(u), min(u), count() FROM
(
    SELECT l.k, uniqExact(l.k) AS u FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j GROUP BY l.k
)
SETTINGS optimize_aggregation_in_order = 1, max_threads = 1;

SELECT 'Sparse key replicated by ARRAY JOIN, attribute read';
SELECT sum(r.v), count(), countIf(r.v = 0), countIf(r.v = (l.j + 1) * 10)
FROM (SELECT j FROM probe_sparse_arr ARRAY JOIN arr) AS l
LEFT JOIN dict_sparse_key AS r ON l.j = r.j
SETTINGS enable_lazy_columns_replication = 1;

-- The whole per-key mapping, not just its total, must equal what a dense key and a non-direct join
-- produce on the same data: a total is invariant under any permutation of values between keys.
-- Grouping by (l.j, r.v) also exposes a key whose rows disagree with each other as extra tuples.
SELECT 'Sparse mapping equals dense mapping';
SELECT
    (SELECT arraySort(groupArray((j, v, c))) FROM
        (SELECT l.j AS j, r.v AS v, count() AS c FROM probe_sparse AS l
         LEFT JOIN dict_sparse_key AS r ON l.j = r.j GROUP BY l.j, r.v))
        = (SELECT arraySort(groupArray((j, v, c))) FROM
            (SELECT l.j AS j, r.v AS v, count() AS c FROM probe_dense AS l
             LEFT JOIN dict_sparse_key AS r ON l.j = r.j GROUP BY l.j, r.v));

SELECT 'Sparse mapping equals hash join mapping';
SELECT
    (SELECT arraySort(groupArray((j, v, c))) FROM
        (SELECT l.j AS j, r.v AS v, count() AS c FROM probe_sparse AS l
         LEFT JOIN dict_sparse_key AS r ON l.j = r.j GROUP BY l.j, r.v))
        = (SELECT arraySort(groupArray((j, v, c))) FROM
            (SELECT l.j AS j, r.v AS v, count() AS c FROM probe_sparse AS l
             LEFT JOIN dict_sparse_key AS r ON l.j = r.j GROUP BY l.j, r.v
             SETTINGS join_algorithm = 'hash'));

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
