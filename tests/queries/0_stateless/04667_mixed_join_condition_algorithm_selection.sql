-- Session SET (not per-statement SETTINGS) so it also covers the oracle subqueries
-- and is not overridden by `compatibility` randomization.
SET enable_analyzer = 1;
SET allow_experimental_join_condition = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS dsrc;
DROP DICTIONARY IF EXISTS dict;

CREATE TABLE t1 (key String, a UInt32) ENGINE = Memory;
CREATE TABLE t2 (key String, a UInt32) ENGINE = Memory;
INSERT INTO t1 VALUES ('k1', 1), ('k1', 2), ('k1', 3);
INSERT INTO t2 VALUES ('k1', 10), ('k1', 20);

-- Oracle, computed without any JOIN algorithm: of the 3x2 pairs sharing the key,
-- only (1, 20) satisfies the residual, so a LEFT JOIN returns 1 matched row plus
-- 2 NULL-extended rows => count 3, sum(t2.a) 20.
SELECT 'oracle_left', count(), sum(t2a) FROM
(
    SELECT t2.a AS t2a FROM t1 CROSS JOIN t2 WHERE (t1.key = t2.key) AND (t1.a * 10 < t2.a)
    UNION ALL
    SELECT NULL FROM t1 WHERE a NOT IN (SELECT t1a FROM (SELECT t1.a AS t1a FROM t1 CROSS JOIN t2 WHERE (t1.key = t2.key) AND (t1.a * 10 < t2.a)))
);

SELECT '-- residual honoured whichever algorithm the list puts first --';

-- A merge algorithm listed before a hash one used to be selected and then silently
-- ignored the mixed condition, returning all 6 key matches with sum 90.
SELECT 'fsm,hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'partial_merge,hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'partial_merge,hash';

SELECT 'prefer_partial_merge,hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'prefer_partial_merge,hash';

SELECT 'fsm,parallel_hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,parallel_hash';

SELECT 'fsm,grace_hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,grace_hash';

-- JoinSwitcher used to swap in MergeJoin once the right side crossed max_rows_in_join,
-- so the same query silently changed its answer as the right table grew.
-- Both external-join settings are pinned because the spilling branch of the AUTO arm is
-- evaluated before the JoinSwitcher one and delegates to a HashJoin, which honours the
-- mixed condition; the ratio defaults to 0.5 and yields a non-zero threshold on any server
-- with a memory limit, so pinning only the absolute setting would leave that branch live.
SELECT 'auto,hash max_rows_in_join=1', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'auto,hash', max_rows_in_join = 1,
         max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT '-- every kind and strictness that builds a mixed condition --';

SELECT 'LEFT ANY', count(), sum(t2.a) FROM t1 LEFT ANY JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'INNER ANY', count(), sum(t2.a) FROM t1 INNER ANY JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'RIGHT ALL', count(), sum(t2.a) FROM t1 RIGHT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'RIGHT ALL residual reversed', count(), sum(t2.a) FROM t1 RIGHT JOIN t2 ON (t1.key = t2.key) AND (t2.a > t1.a * 10)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'FULL ALL rows', count() FROM t1 FULL JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

-- MergeJoin, unlike FullSortingMergeJoin, does admit Left + Semi, so partial_merge is the
-- list that reaches its capability predicate with a Semi strictness.
SELECT 'LEFT SEMI partial_merge,hash', count() FROM t1 LEFT SEMI JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'partial_merge,hash';

-- ASOF consumes only the one closest-match inequality, so a second cross-side predicate still
-- becomes a mixed condition. FullSortingMergeJoin admits ASOF on strictness, so before this fix
-- the merge algorithm was selected and dropped that predicate silently: the closest t2 row for
-- t1.a = 1 is 10, which `1 * 10 != 10` excludes, yet the merge path still matched it (sum 30
-- where honouring the predicate gives 20). Now the predicate declines, the list falls through to
-- the hash family, and that reports it cannot evaluate a non-equi condition for ASOF strictness.
SELECT count() FROM t1 ASOF LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a <= t2.a) AND (t1.a * 10 != t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Plain ASOF, with no second predicate, builds no mixed condition and keeps the merge path.
SELECT 'ASOF plain keeps merge', countIf(explain LIKE '%MergeJoinTransform%') FROM
(
    EXPLAIN PIPELINE SELECT count() FROM t1 ASOF LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a <= t2.a)
    SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_optimize_join_order_randomize = 0
);

SELECT '-- the hash family was already correct and must stay so --';

SELECT 'hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'hash';

SELECT 'parallel_hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'parallel_hash';

SELECT 'grace_hash', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'grace_hash';

SELECT 'default list', count(), sum(t2.a) FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'direct,parallel_hash,hash';

-- INNER ALL pushes the residual down to a post-join filter instead of building a mixed
-- condition, so it is algorithm-independent and reads 1 matched row both ways.
SELECT 'INNER ALL fsm,hash', count(), sum(t2.a) FROM t1 INNER JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'INNER ALL hash', count(), sum(t2.a) FROM t1 INNER JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'hash';

-- FullSortingMergeJoin declines Semi and Anti on strictness alone, so these two reach the
-- hash arm either way; they are must-not-regress rows for that pre-existing rejection.
SELECT 'LEFT SEMI fsm,hash', count() FROM t1 LEFT SEMI JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'LEFT ANTI fsm,hash', count() FROM t1 LEFT ANTI JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT '-- rejection when the list has no hash family member is unchanged --';

SELECT count() FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError NOT_IMPLEMENTED }

SELECT count() FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'partial_merge'; -- { serverError NOT_IMPLEMENTED }

SELECT count() FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'prefer_partial_merge'; -- { serverError NOT_IMPLEMENTED }

SELECT count() FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
SETTINGS join_algorithm = 'auto'; -- { serverError NOT_IMPLEMENTED }

SELECT '-- a merge algorithm is still selected where it is valid --';

-- Structural, not by value: 6/90 is ambiguous, so assert which transform the pipeline
-- actually contains. Plain equi and one-sided residuals keep the merge path; only the
-- mixed condition falls through to hash.
-- query_plan_optimize_join_order_randomize is pinned because a non-zero value replaces
-- the relation statistics with seeded random ones, which can swap the join sides and
-- drop the merge path for reasons unrelated to this fix.
SELECT 'equi keeps merge', countIf(explain LIKE '%MergeJoinTransform%') FROM
(
    EXPLAIN PIPELINE SELECT count() FROM t1 LEFT JOIN t2 ON t1.key = t2.key
    SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_optimize_join_order_randomize = 0
);

SELECT 'one-sided keeps merge', countIf(explain LIKE '%MergeJoinTransform%') FROM
(
    EXPLAIN PIPELINE SELECT count() FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t2.a > 15)
    SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_optimize_join_order_randomize = 0
);

SELECT 'mixed falls through', countIf(explain LIKE '%MergeJoinTransform%') FROM
(
    EXPLAIN PIPELINE SELECT count() FROM t1 LEFT JOIN t2 ON (t1.key = t2.key) AND (t1.a * 10 < t2.a)
    SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_optimize_join_order_randomize = 0
);

SELECT '-- direct join: reachable with no non-default setting at all --';

CREATE TABLE dsrc (key UInt64, a UInt32) ENGINE = Memory;
INSERT INTO dsrc VALUES (1, 10), (2, 20);
CREATE DICTIONARY dict (key UInt64, a UInt32) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'dsrc')) LIFETIME(0) LAYOUT(FLAT());

DROP TABLE IF EXISTS t3;
CREATE TABLE t3 (key UInt64, a UInt32) ENGINE = Memory;
INSERT INTO t3 VALUES (1, 1), (2, 2), (3, 3);

-- join_algorithm defaults to 'direct,parallel_hash,hash', so DirectKeyValueJoin is tried
-- first and used to drop the residual. No pair satisfies a * 10 < d.a here, so every left
-- row is unmatched. A dictionary fills unmatched rows with the attribute default (0)
-- rather than NULL, so the oracle uses 0 for them too => count 3, sum 0.
SELECT 'oracle_dict', count(), sum(da) FROM
(
    SELECT dsrc.a AS da FROM t3 CROSS JOIN dsrc WHERE (t3.key = dsrc.key) AND (t3.a * 10 < dsrc.a)
    UNION ALL
    SELECT toUInt32(0) FROM t3 WHERE a NOT IN (SELECT t3a FROM (SELECT t3.a AS t3a FROM t3 CROSS JOIN dsrc WHERE (t3.key = dsrc.key) AND (t3.a * 10 < dsrc.a)))
);

SELECT 'dict LEFT ANY, no settings', count(), sum(d.a) FROM t3 LEFT ANY JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a);

SELECT 'dict LEFT ALL, no settings', count(), sum(d.a) FROM t3 LEFT JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a);

-- tryDirectJoin also admits Left + Semi and Left + Anti, so these two Semi/Anti shapes reach
-- a declining predicate with no non-default setting at all. No pair satisfies the residual,
-- so SEMI keeps no left row and ANTI keeps all three.
SELECT 'dict LEFT SEMI, no settings', count() FROM t3 LEFT SEMI JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a);

SELECT 'dict LEFT ANTI, no settings', count() FROM t3 LEFT ANTI JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a);

SELECT 'dict LEFT ANY hash', count(), sum(d.a) FROM t3 LEFT ANY JOIN dict AS d ON (t3.key = d.key) AND (t3.a * 10 < d.a)
SETTINGS join_algorithm = 'hash';

-- The direct join must still be selected for a plain equi join onto the dictionary.
SELECT 'dict equi keeps direct', countIf(explain LIKE '%FilledJoin%') FROM
(
    EXPLAIN PIPELINE SELECT count() FROM t3 LEFT ANY JOIN dict AS d ON t3.key = d.key
);

SELECT 'dict equi value', count(), sum(d.a) FROM t3 LEFT ANY JOIN dict AS d ON t3.key = d.key;

DROP DICTIONARY dict;
DROP TABLE dsrc;
DROP TABLE t3;
DROP TABLE t2;
DROP TABLE t1;
