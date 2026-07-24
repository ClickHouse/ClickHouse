DROP TABLE IF EXISTS t_mutation_udf_subquery;
DROP FUNCTION IF EXISTS udf_mutation_in_subquery;

CREATE TABLE t_mutation_udf_subquery (d Date, id UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY (d, id);

INSERT INTO t_mutation_udf_subquery SELECT '2024-01-01', number, 0 FROM numbers(100);
INSERT INTO t_mutation_udf_subquery SELECT '2024-02-01', 100 + number, 0 FROM numbers(100);

-- The UDF body hides a subquery that only TreeRewriter's UDF substitution exposes. The index-analysis
-- fast path must run that substitution before its subquery guard and then bail, so the set is not built
-- here and again by the fallback query.
CREATE FUNCTION udf_mutation_in_subquery AS (x) -> x IN (SELECT number FROM numbers(7, 2));

SET mutations_sync = 2;

ALTER TABLE t_mutation_udf_subquery UPDATE v = 1 WHERE udf_mutation_in_subquery(id);

SELECT sum(v), count() FROM t_mutation_udf_subquery;

SYSTEM FLUSH LOGS part_log;

-- No part is skipped by index analysis: the subquery guard fires after UDF substitution.
SELECT sum(ProfileEvents['MutationUntouchedPartsByIndexAnalysis'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_mutation_udf_subquery' AND event_type = 'MutatePart';

DROP FUNCTION udf_mutation_in_subquery;
DROP TABLE t_mutation_udf_subquery;
