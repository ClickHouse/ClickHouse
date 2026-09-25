-- `join_algorithm` is a preference list, and without a spill threshold `grace_hash` in it is passed
-- over for the next algorithm. So under `join_algorithm = 'full_sorting_merge,grace_hash'` only
-- `full_sorting_merge` can run, and it implements neither `SEMI` nor `ANTI`: converting an `ANY` join
-- into one of them must not happen there, or a query that runs fails with `NOT_IMPLEMENTED`.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
-- CI may inject False; pin it so the conversion pass under test always runs.
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
-- No spill threshold, so `grace_hash` is skipped whenever another algorithm follows or precedes it.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, legacy_join_size_limits_trigger_spilling = 0;

DROP TABLE IF EXISTS t_grace_l;
DROP TABLE IF EXISTS t_grace_r;
CREATE TABLE t_grace_l (k UInt32, a UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_grace_r (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_grace_l SELECT number, number % 7 FROM numbers(100);
INSERT INTO t_grace_r SELECT number * 2, number % 3 FROM numbers(50);

SELECT 'an ANY LEFT JOIN filtered on the right side';
SELECT count() FROM t_grace_l ANY LEFT JOIN t_grace_r USING (k) WHERE t_grace_r.v > 0 SETTINGS join_algorithm = 'full_sorting_merge,grace_hash';
SELECT count() FROM t_grace_l ANY LEFT JOIN t_grace_r USING (k) WHERE t_grace_r.v > 0 SETTINGS join_algorithm = 'hash';

SELECT 'a correlated EXISTS and NOT EXISTS';
SELECT count() FROM t_grace_l AS o WHERE EXISTS (SELECT 1 FROM t_grace_l AS i WHERE i.a = o.a AND i.k = 5) SETTINGS join_algorithm = 'full_sorting_merge,grace_hash';
SELECT count() FROM t_grace_l AS o WHERE NOT EXISTS (SELECT 1 FROM t_grace_l AS i WHERE i.a = o.a AND i.k = 5) SETTINGS join_algorithm = 'full_sorting_merge,grace_hash';
-- The decorrelation builds an `ANY RIGHT` join that `partial_merge` cannot execute either.
SELECT count() FROM t_grace_l AS o WHERE EXISTS (SELECT 1 FROM t_grace_l AS i WHERE i.a = o.a AND i.k = 5) SETTINGS join_algorithm = 'grace_hash,partial_merge';
SELECT count() FROM t_grace_l AS o WHERE NOT EXISTS (SELECT 1 FROM t_grace_l AS i WHERE i.a = o.a AND i.k = 5) SETTINGS join_algorithm = 'grace_hash,partial_merge';

SELECT 'the strictness the plan ends up with';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, description = 1
    SELECT count() FROM t_grace_l ANY LEFT JOIN t_grace_r USING (k) WHERE t_grace_r.v > 0
    SETTINGS join_algorithm = 'full_sorting_merge,grace_hash') WHERE explain LIKE '%Strictness%';
-- With a spill threshold `grace_hash` can run, so the conversion happens.
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, description = 1
    SELECT count() FROM t_grace_l ANY LEFT JOIN t_grace_r USING (k) WHERE t_grace_r.v > 0
    SETTINGS join_algorithm = 'full_sorting_merge,grace_hash', max_bytes_before_external_join = 1000000000) WHERE explain LIKE '%Strictness%';

DROP TABLE t_grace_l;
DROP TABLE t_grace_r;
