-- The logical join step's general planning (`buildDisjunctiveJoinConditionsGeneral`) with several
-- disjuncts, and with conditions the usual planning cannot split, under
-- `join_algorithm = 'partitioned_hash'` alone. The plan is the one `hash` gets. The algorithm is
-- `PartitionedHashJoin` (several disjuncts run on its delegated build). The rows are those of `hash`.

SET enable_analyzer = 1;
SET allow_general_join_planning = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_join_runtime_filters = 0, query_plan_join_swap_table = false, query_plan_merge_filter_into_join_condition = 0;
SET optimize_extract_common_expressions = 1;

DROP TABLE IF EXISTS t_phj_lp_l;
DROP TABLE IF EXISTS t_phj_lp_r;
CREATE TABLE t_phj_lp_l (id UInt64, key String, key2 String, b UInt64) ENGINE = Memory
    AS SELECT number, toString(number % 3), toString(number % 3), number % 7 FROM numbers(60);
CREATE TABLE t_phj_lp_r (id UInt64, key String, key2 String, c UInt64) ENGINE = Memory
    AS SELECT number % 40, toString(number % 4), toString(number % 2), number % 5 FROM numbers(60);

SELECT '-- two disjuncts, the second with a one-sided condition';
SELECT replaceRegexpAll(explain, '^[ │└─]+', '') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_lp_l AS l INNER JOIN t_phj_lp_r AS r ON l.id = r.id OR (l.b = r.c AND l.id > 40)
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Algorithm%' OR explain LIKE '%Filter column%';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_lp_l AS l INNER JOIN t_phj_lp_r AS r ON l.id = r.id OR (l.b = r.c AND l.id > 40) SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_lp_l AS l INNER JOIN t_phj_lp_r AS r ON l.id = r.id OR (l.b = r.c AND l.id > 40) SETTINGS join_algorithm = 'hash';

SELECT '-- a common equality factored out of two disjuncts, with per-side conditions (the shape of 01881)';
SELECT replaceRegexpAll(explain, '^[ │└─]+', '') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_phj_lp_l AS l INNER JOIN t_phj_lp_r AS r
    ON r.key = r.key2 AND l.key = l.key2 AND l.key != 'XXX' AND l.id = r.id OR r.key = r.key2 AND l.id = r.id AND l.id = r.id
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Algorithm%' OR explain LIKE '%Filter column%';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_lp_l AS l INNER JOIN t_phj_lp_r AS r
    ON r.key = r.key2 AND l.key = l.key2 AND l.key != 'XXX' AND l.id = r.id OR r.key = r.key2 AND l.id = r.id AND l.id = r.id
    SETTINGS join_algorithm = 'partitioned_hash';
SELECT count(), sum(l.id * 1000 + r.c) FROM t_phj_lp_l AS l INNER JOIN t_phj_lp_r AS r
    ON r.key = r.key2 AND l.key = l.key2 AND l.key != 'XXX' AND l.id = r.id OR r.key = r.key2 AND l.id = r.id AND l.id = r.id
    SETTINGS join_algorithm = 'hash';

SELECT '-- FULL join with two disjuncts: the non-joined rows of both sides';
SELECT count(), sum(ifNull(l.id, 0) * 1000 + ifNull(r.c, 0)), countIf(l.id IS NULL), countIf(r.id IS NULL)
    FROM t_phj_lp_l AS l FULL JOIN t_phj_lp_r AS r ON l.id = r.id OR l.b = r.c
    SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1;
SELECT count(), sum(ifNull(l.id, 0) * 1000 + ifNull(r.c, 0)), countIf(l.id IS NULL), countIf(r.id IS NULL)
    FROM t_phj_lp_l AS l FULL JOIN t_phj_lp_r AS r ON l.id = r.id OR l.b = r.c
    SETTINGS join_algorithm = 'hash', join_use_nulls = 1;

DROP TABLE t_phj_lp_l;
DROP TABLE t_phj_lp_r;
