DROP TABLE IF EXISTS t_mutation_index_analysis;

CREATE TABLE t_mutation_index_analysis (d Date, id UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY (d, id);

INSERT INTO t_mutation_index_analysis SELECT '2024-01-01', number, 0 FROM numbers(100);
INSERT INTO t_mutation_index_analysis SELECT '2024-02-01', 100 + number, 0 FROM numbers(100);

SET mutations_sync = 2;

-- Part 202402 is proven untouched by the primary key index.
ALTER TABLE t_mutation_index_analysis UPDATE v = 1 WHERE id = 50;

-- Both parts are proven untouched by partition pruning.
ALTER TABLE t_mutation_index_analysis DELETE WHERE d = '2030-01-01';

-- Part 202402 is proven untouched by the primary key index with an explicit set.
ALTER TABLE t_mutation_index_analysis UPDATE v = 2 WHERE id IN (7, 8);

-- Both parts are proven untouched by the minmax index on the partition column.
ALTER TABLE t_mutation_index_analysis DELETE WHERE d = '2024-01-15';

-- A predicate on a non-key column is not provable by index analysis and is checked by the fallback query.
ALTER TABLE t_mutation_index_analysis UPDATE v = 3 WHERE v = 999;

-- The partition matches, and part 202402 is proven untouched by the primary key index.
ALTER TABLE t_mutation_index_analysis UPDATE v = 4 IN PARTITION 202402 WHERE id = 5;

-- The partition matches and one row is updated.
ALTER TABLE t_mutation_index_analysis UPDATE v = 5 IN PARTITION 202402 WHERE id = 105;

SELECT sum(v), count() FROM t_mutation_index_analysis;

SYSTEM FLUSH LOGS part_log;

SELECT
    sum(ProfileEvents['MutationUntouchedPartsByIndexAnalysis']),
    countIf(ProfileEvents['MutationUntouchedPartsByIndexAnalysis'] > 0 AND ProfileEvents['QueryPlanOptimizeMicroseconds'] > 0)
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_mutation_index_analysis' AND event_type = 'MutatePart';

DROP TABLE t_mutation_index_analysis;

-- The part minmax bound skips NaN, so a negated range does not prove a part with a NaN row untouched.
DROP TABLE IF EXISTS t_mutation_index_analysis_nan;
CREATE TABLE t_mutation_index_analysis_nan (id UInt64, val Nullable(Float64), v UInt64)
ENGINE = MergeTree PARTITION BY isNull(val) ORDER BY id;
INSERT INTO t_mutation_index_analysis_nan VALUES (1, 1., 0), (2, nan, 0), (3, 3., 0);
ALTER TABLE t_mutation_index_analysis_nan UPDATE v = 1 WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT sum(v) FROM t_mutation_index_analysis_nan;
ALTER TABLE t_mutation_index_analysis_nan DELETE WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_mutation_index_analysis_nan;
DROP TABLE t_mutation_index_analysis_nan;
