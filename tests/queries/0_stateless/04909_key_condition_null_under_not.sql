-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: the EXPLAIN assertions below require the exact-count projection to
--   short-circuit on the initiator, which is suppressed when reading from remote replicas.
-- no-replicated-database: EXPLAIN output differs for replicated database.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t;
-- The projection assertions below depend on whether a granule lies wholly inside the range the
-- condition is definitely true for, so the granularity has to be fixed.
CREATE TABLE t (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO t SELECT number FROM numbers(40);

-- An all-NULL expression under NOT must not make the range provably true.
SELECT count() FROM t WHERE NOT (id >= 20 AND NULL);
SELECT count() FROM t WHERE NOT (id >= 20 AND toInt64OrNull('x'));
SELECT count() FROM t WHERE NOT (id >= 20 AND CAST(NULL, 'Nullable(UInt8)'));
SELECT count() FROM t WHERE NOT (id >= 20 AND if(id % 2, NULL, NULL));
SELECT count() FROM t WHERE NOT (id >= 20 AND materialize(NULL));
SELECT length(groupArray(id)) FROM t WHERE NOT (id >= 20 AND NULL);
SELECT count() FROM t WHERE NOT (id >= 20 AND NULL) SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM (SELECT number FROM numbers(100) WHERE NOT (number = 5 AND NULL) LIMIT 10);

-- The exact-count projection is still used where the range really is provably true,
-- and is declined for the shape above.
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t WHERE id < 1000) WHERE explain ILIKE '%_exact_count_projection%';
SELECT count() FROM t WHERE id < 1000;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t WHERE NOT (id >= 20 AND NULL)) WHERE explain ILIKE '%_exact_count_projection%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE NOT (id >= 20 AND NULL)) WHERE explain ILIKE '%Condition:%id%';

-- A positive all-NULL conjunct stays exact, so an unbounded source must not be read.
SELECT count() FROM t WHERE id >= 20 AND NULL;
SELECT count() FROM (SELECT number FROM numbers() WHERE number > 0 AND NULL LIMIT 10);
SELECT count() FROM (SELECT number FROM numbers() WHERE number > 0 AND toInt64OrNull('x') LIMIT 10);
SELECT count() FROM t WHERE NOT NULL;
SELECT count() FROM t WHERE NOT (id >= 20 OR NULL);
SELECT count() FROM t WHERE NOT (NOT (id >= 20 AND NULL));

-- Where the negated expression supplies a value rather than a condition, its type must survive:
-- ifNull(NOT NULL, 0) is UInt8, and reinterpreting a wider type would change the value.
SELECT DISTINCT length(reinterpretAsFixedString(ifNull(NOT materialize(CAST(NULL, 'Nullable(Int64)')), 0))) FROM t;
SELECT length(groupArray(id)) FROM t WHERE length(reinterpretAsFixedString(ifNull(NOT materialize(CAST(NULL, 'Nullable(Int64)')), 0))) = 1 SETTINGS optimize_use_implicit_projections = 0;
SELECT DISTINCT length(reinterpretAsFixedString(ifNull(NOT materialize(CAST(NULL, 'Nullable(Float64)')), 0))) FROM t;
SELECT length(groupArray(id)) FROM t WHERE length(reinterpretAsFixedString(ifNull(NOT materialize(CAST(NULL, 'Nullable(Float64)')), 0))) = 1 SETTINGS optimize_use_implicit_projections = 0;

-- A row the predicate is NULL for must not be deleted by a mutation using that predicate.
ALTER TABLE t DELETE WHERE NOT (id >= 38 AND NULL) SETTINGS mutations_sync = 2;
SELECT arraySort(groupArray(id)) FROM t;
