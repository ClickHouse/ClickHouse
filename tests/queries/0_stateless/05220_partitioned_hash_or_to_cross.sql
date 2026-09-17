-- An INNER ALL join whose ON expression has no equality key is converted by the logical join step
-- into a CROSS join with the expression as a filter. That conversion is `tryAddDisjunctiveConditions`
-- in `JoinStepLogical.cpp`. It runs when a hash family algorithm is enabled. `partitioned_hash`
-- alone qualifies now. The cross join itself is executed by `ConstantJoin`, the one implementation of
-- CROSS joins, whatever `join_algorithm` lists. The plan therefore shows that algorithm, not
-- `PartitionedHashJoin`, and the rows are those of `hash`. Strictness ANY is not converted for
-- either algorithm.

SET enable_analyzer = 1;
SET allow_general_join_planning = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_join_runtime_filters = 0, query_plan_join_swap_table = false, query_plan_merge_filter_into_join_condition = 0;

DROP TABLE IF EXISTS t_phj_otc_l;
DROP TABLE IF EXISTS t_phj_otc_r;
CREATE TABLE t_phj_otc_l (id UInt64, b UInt64) ENGINE = Memory AS SELECT number, number % 7 FROM numbers(60);
CREATE TABLE t_phj_otc_r (id UInt64, c UInt64) ENGINE = Memory AS SELECT number % 40, number % 5 FROM numbers(60);

SELECT '-- OR of two inequalities: CROSS join plus a filter, executed by ConstantJoin';
SELECT replaceRegexpAll(explain, '^[ │└─]+', '') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_otc_l AS l INNER JOIN t_phj_otc_r AS r ON l.b < r.c OR l.id > r.id + 30
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Algorithm%' OR explain LIKE '%Filter column%';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_otc_l AS l INNER JOIN t_phj_otc_r AS r ON l.b < r.c OR l.id > r.id + 30 SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_otc_l AS l INNER JOIN t_phj_otc_r AS r ON l.b < r.c OR l.id > r.id + 30 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_otc_l AS l CROSS JOIN t_phj_otc_r AS r WHERE l.b < r.c OR l.id > r.id + 30;

SELECT '-- a single inequality';
SELECT replaceRegexpAll(explain, '^[ │└─]+', '') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_otc_l AS l INNER JOIN t_phj_otc_r AS r ON l.b > r.c
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Algorithm%' OR explain LIKE '%Filter column%';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_otc_l AS l INNER JOIN t_phj_otc_r AS r ON l.b > r.c SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_otc_l AS l INNER JOIN t_phj_otc_r AS r ON l.b > r.c SETTINGS join_algorithm = 'hash';

SELECT '-- INNER ANY is not converted, for either algorithm';
SELECT count() FROM t_phj_otc_l AS l INNER ANY JOIN t_phj_otc_r AS r ON l.b < r.c OR l.id > r.id + 30 SETTINGS join_algorithm = 'partitioned_hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM t_phj_otc_l AS l INNER ANY JOIN t_phj_otc_r AS r ON l.b < r.c OR l.id > r.id + 30 SETTINGS join_algorithm = 'hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE t_phj_otc_l;
DROP TABLE t_phj_otc_r;
