-- Tags: no-parallel-replicas
-- The direct join algorithm is not available with parallel replicas.

DROP DICTIONARY IF EXISTS dict_sparse_key;
DROP TABLE IF EXISTS probe_sparse;
DROP TABLE IF EXISTS probe_dense;
DROP TABLE IF EXISTS probe_sparse_arr;
DROP TABLE IF EXISTS dict_source;

-- The attribute is never 0, so with join_use_nulls = 0 the value `r.v = 0` means "key not found".
CREATE TABLE dict_source (j UInt64, v UInt64) ENGINE = MergeTree ORDER BY j;
INSERT INTO dict_source SELECT number, (number + 1) * 10 FROM numbers(20);

CREATE DICTIONARY dict_sparse_key (j UInt64, v UInt64) PRIMARY KEY j
SOURCE(CLICKHOUSE(TABLE 'dict_source' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

-- `j` is default-heavy, so it is serialized Sparse. Keys 0..19 are in the dictionary, 20..24 are not.
CREATE TABLE probe_sparse (k UInt32, j UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO probe_sparse SELECT number % 50, number % 25 FROM numbers(4000);

-- The same data written densely, used as the expected result below.
CREATE TABLE probe_dense (k UInt32, j UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO probe_dense SELECT number % 50, number % 25 FROM numbers(4000);

-- A sparse `j` carried through ARRAY JOIN, so the join key arrives replicated rather than plain.
CREATE TABLE probe_sparse_arr (j UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO probe_sparse_arr SELECT number % 25, [1, 2, 3] FROM numbers(1500);

SELECT 'The join keys are really serialized Sparse';
SELECT table, countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('probe_sparse', 'probe_sparse_arr')
  AND column = 'j' AND active
GROUP BY table ORDER BY table;

SET join_algorithm = 'direct';
-- Pinned, not left to randomization: a miss must read as 0 rather than NULL below.
SET join_use_nulls = 0;

-- `r.j` observes which keys were found; the two `l.j` counts catch a key classified the wrong way.
SELECT 'Sparse key, key presence only';
SELECT count(), countIf(r.v != 0), countIf(r.v = 0), countIf(l.j = 0 AND r.v = 10),
       countIf(l.j < 20 AND r.j = l.j), countIf(l.j >= 20 AND r.j = 0)
FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j;

-- `r.v = (l.j + 1) * 10` restates the dictionary contents, so values swapped between keys are
-- caught even though the sum stays the same.
SELECT 'Sparse key, dictionary attribute read';
SELECT sum(r.v), count(), countIf(r.v = (l.j + 1) * 10), countIf(r.v = 0 AND l.j >= 20)
FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j;

-- Key 0 is the sparse default and an unmatched right key is 0 too, so join_use_nulls = 1 is what
-- separates a found key 0 from a lost one here.
SELECT 'Sparse default key 0 is really found';
SELECT countIf(r.j IS NULL), countIf(l.j < 20 AND r.j IS NOT NULL), countIf(l.j = 0 AND r.j = 0)
FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j
SETTINGS join_use_nulls = 1;

SELECT 'Sparse key, aggregation in order';
SELECT max(u), min(u), count() FROM
(
    SELECT l.k, uniqExact(l.k) AS u FROM probe_sparse AS l LEFT JOIN dict_sparse_key AS r ON l.j = r.j GROUP BY l.k
)
SETTINGS optimize_aggregation_in_order = 1, max_threads = 1;

SELECT 'Sparse key replicated by ARRAY JOIN, attribute read';
SELECT sum(r.v), count(), countIf(r.v = 0), countIf(r.v = (l.j + 1) * 10)
FROM (SELECT j FROM probe_sparse_arr ARRAY JOIN arr) AS l
LEFT JOIN dict_sparse_key AS r ON l.j = r.j;

-- Comparing the whole per-key mapping, not a total: a total survives values swapped between keys.
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
