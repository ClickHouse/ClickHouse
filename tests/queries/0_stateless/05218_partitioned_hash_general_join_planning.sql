-- `partitioned_hash` is a member of the hash join family. The general join planning of the logical
-- join step (`buildDisjunctiveJoinConditionsGeneral` in `PlannerJoinsLogical.cpp`) therefore applies
-- to it. The ON expression is split into equality clauses, one-sided filters and a residual
-- condition, exactly as for `hash`. The two shapes below are planned the same way for both
-- algorithms and return the same rows. The second shape used to leave a post-join `Filter` step
-- when `partitioned_hash` was planned by the usual path.
-- `buildJoinClausesAndActions` in `PlannerJoins.cpp` is the other general planning entry. It has no
-- caller since the logical join step became the only join planner, so this test does not exercise it.

SET enable_analyzer = 1;
SET allow_general_join_planning = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_join_runtime_filters = 0, query_plan_join_swap_table = false, query_plan_merge_filter_into_join_condition = 0;
SET optimize_extract_common_expressions = 1;

DROP TABLE IF EXISTS t_phj_gjp_l;
DROP TABLE IF EXISTS t_phj_gjp_r;
CREATE TABLE t_phj_gjp_l (id UInt64, b UInt64) ENGINE = Memory AS SELECT number, number % 7 FROM numbers(60);
CREATE TABLE t_phj_gjp_r (id UInt64, c UInt64) ENGINE = Memory AS SELECT number % 40, number % 5 FROM numbers(60);

SELECT '-- one-sided conditions: one clause, two per-side filters, partitioned_hash serves the join';
SELECT replaceRegexpAll(explain, '^[ │└─]+', '') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND l.b < 3 AND r.c > 2
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Algorithm%' OR explain LIKE '%Filter column%';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND l.b < 3 AND r.c > 2 SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND l.b < 3 AND r.c > 2 SETTINGS join_algorithm = 'hash';

SELECT '-- two-sided residual: folded into the join by the general planning, no post-join filter, same plan as hash';
SELECT replaceRegexpAll(explain, '^[ │└─]+', '') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND (l.b < 3 OR r.c > 2)
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Algorithm%' OR explain LIKE '%Filter column%';
SELECT 'post-join filter steps, partitioned_hash then hash:', count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND (l.b < 3 OR r.c > 2)
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Filter column%';
SELECT 'post-join filter steps, partitioned_hash then hash:', count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND (l.b < 3 OR r.c > 2)
    SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%Filter column%';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND (l.b < 3 OR r.c > 2) SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l INNER JOIN t_phj_gjp_r AS r ON l.id = r.id AND (l.b < 3 OR r.c > 2) SETTINGS join_algorithm = 'hash';

SELECT '-- the same two shapes as LEFT joins';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l LEFT JOIN t_phj_gjp_r AS r ON l.id = r.id AND l.b < 3 AND r.c > 2 SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l LEFT JOIN t_phj_gjp_r AS r ON l.id = r.id AND l.b < 3 AND r.c > 2 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l LEFT JOIN t_phj_gjp_r AS r ON l.id = r.id AND (l.b < 3 OR r.c > 2) SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_gjp_l AS l LEFT JOIN t_phj_gjp_r AS r ON l.id = r.id AND (l.b < 3 OR r.c > 2) SETTINGS join_algorithm = 'hash';

DROP TABLE t_phj_gjp_l;
DROP TABLE t_phj_gjp_r;
