SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt32) ENGINE = MergeTree ORDER BY id;
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

-- A positive all-NULL conjunct stays exact, so an unbounded source must not be read.
SELECT count() FROM t WHERE id >= 20 AND NULL;
SELECT count() FROM (SELECT number FROM numbers() WHERE number > 0 AND NULL LIMIT 10);
SELECT count() FROM (SELECT number FROM numbers() WHERE number > 0 AND toInt64OrNull('x') LIMIT 10);
SELECT count() FROM t WHERE NOT NULL;
SELECT count() FROM t WHERE NOT (id >= 20 OR NULL);
SELECT count() FROM t WHERE NOT (NOT (id >= 20 AND NULL));

-- A row the predicate is NULL for must not be deleted by a mutation using that predicate.
ALTER TABLE t DELETE WHERE NOT (id >= 38 AND NULL) SETTINGS mutations_sync = 2;
SELECT groupArray(id) FROM t;
