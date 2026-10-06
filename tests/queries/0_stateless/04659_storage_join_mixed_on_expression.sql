-- The Join table engine reuses one prebuilt join, so it cannot serve an ON condition that
-- references both tables. Such queries used to reach inconsistent state and fail with a
-- logical error, std::bad_variant_access, or a misleading "with ORs" message.
-- enable_analyzer = 1 is load-bearing: the old analyzer rejects this shape earlier with a
-- different error, so without the pin the test stops exercising the guard.
SET enable_analyzer = 1;
SET allow_experimental_join_condition = 1;
-- join_algorithm is load-bearing: chooseJoinAlgorithm rejects a mixed ON condition outright for
-- any algorithm other than hash, parallel_hash or grace_hash, and it does so before reaching the
-- Join-engine branch, so without this pin a randomized join_algorithm changes the expected errors.
SET join_algorithm = 'hash';
-- join_use_nulls is load-bearing: the Join engine captures it at CREATE time from the global
-- context while the query reads the session value, so a session join_use_nulls = 1 makes
-- getJoinLocked reject every LEFT or FULL join against the engine with its own
-- "needs the same join_use_nulls setting" error, upstream of the guard under test.
SET join_use_nulls = 0;
-- enable_parallel_replicas is load-bearing: with parallel replicas the Join engine is read as a
-- plain source and sent to the replicas as a temporary table, so getJoinLocked is not called at
-- all and none of its rejections apply, neither the new one nor the pre-existing ones.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS sj_all_left;
DROP TABLE IF EXISTS sj_all_left_2col;
DROP TABLE IF EXISTS sj_all_left_nullable;
DROP TABLE IF EXISTS sj_all_left_lc;
DROP TABLE IF EXISTS t1_lc;
DROP TABLE IF EXISTS sj_any_left;
DROP TABLE IF EXISTS sj_semi_left;
DROP TABLE IF EXISTS sj_anti_left;
DROP TABLE IF EXISTS sj_any_inner;
DROP TABLE IF EXISTS sj_all_inner;
DROP TABLE IF EXISTS sj_all_inner_lc;
DROP TABLE IF EXISTS sj_all_right;
DROP TABLE IF EXISTS sj_all_full;
DROP TABLE IF EXISTS mt_right;
DROP TABLE IF EXISTS mem_right;
DROP TABLE IF EXISTS sj_bool;
DROP TABLE IF EXISTS sj_bool_nullable;

CREATE TABLE t1 (key String, a UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1 VALUES ('k1', 1), ('k1', 2), ('k1', 3);

CREATE TABLE sj_all_left (key String, a UInt64) ENGINE = Join(ALL, LEFT, key);
INSERT INTO sj_all_left VALUES ('k1', 10), ('k1', 20);

CREATE TABLE sj_all_left_2col (key String, a UInt64, b UInt64) ENGINE = Join(ALL, LEFT, key);
INSERT INTO sj_all_left_2col VALUES ('k1', 10, 100), ('k1', 20, 200);

CREATE TABLE sj_all_left_nullable (key String, a Nullable(UInt64)) ENGINE = Join(ALL, LEFT, key);
INSERT INTO sj_all_left_nullable VALUES ('k1', 10), ('k1', 20);

CREATE TABLE t1_lc (key LowCardinality(String), a UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1_lc VALUES ('k1', 1), ('k1', 2), ('k1', 3);
CREATE TABLE sj_all_left_lc (key LowCardinality(String), a UInt64) ENGINE = Join(ALL, LEFT, key);
INSERT INTO sj_all_left_lc VALUES ('k1', 10), ('k1', 20);

CREATE TABLE sj_any_left (key String, a UInt64) ENGINE = Join(ANY, LEFT, key);
INSERT INTO sj_any_left VALUES ('k1', 10), ('k1', 20);
CREATE TABLE sj_semi_left (key String, a UInt64) ENGINE = Join(SEMI, LEFT, key);
INSERT INTO sj_semi_left VALUES ('k1', 10), ('k1', 20);
CREATE TABLE sj_anti_left (key String, a UInt64) ENGINE = Join(ANTI, LEFT, key);
INSERT INTO sj_anti_left VALUES ('k1', 10), ('k1', 20);
CREATE TABLE sj_any_inner (key String, a UInt64) ENGINE = Join(ANY, INNER, key);
INSERT INTO sj_any_inner VALUES ('k1', 10), ('k1', 20);
CREATE TABLE sj_all_inner (key String, a UInt64) ENGINE = Join(ALL, INNER, key);
INSERT INTO sj_all_inner VALUES ('k1', 10), ('k1', 20);
CREATE TABLE sj_all_inner_lc (key LowCardinality(String), a UInt64) ENGINE = Join(ALL, INNER, key);
INSERT INTO sj_all_inner_lc VALUES ('k1', 10), ('k1', 20);
CREATE TABLE sj_all_right (key String, a UInt64) ENGINE = Join(ALL, RIGHT, key);
INSERT INTO sj_all_right VALUES ('k1', 10), ('k1', 20);
CREATE TABLE sj_all_full (key String, a UInt64) ENGINE = Join(ALL, FULL, key);
INSERT INTO sj_all_full VALUES ('k1', 10), ('k1', 20);

CREATE TABLE mt_right (key String, a UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO mt_right VALUES ('k1', 10), ('k1', 20);
CREATE TABLE mem_right (key String, a UInt64) ENGINE = Memory;
INSERT INTO mem_right VALUES ('k1', 10), ('k1', 20);

-- flag distinguishes the two right rows, so a condition on it selects exactly one of them.
CREATE TABLE sj_bool (key String, a UInt64, flag UInt8) ENGINE = Join(ALL, INNER, key);
INSERT INTO sj_bool VALUES ('k1', 10, 0), ('k1', 20, 1);
CREATE TABLE sj_bool_nullable (key String, a UInt64, flag Nullable(UInt8)) ENGINE = Join(ALL, INNER, key);
INSERT INTO sj_bool_nullable VALUES ('k1', 10, 0), ('k1', 20, 1), ('k1', 30, NULL);

SELECT '--- rejected: a mixed ON condition on a Join-engine table ---';

-- R1..R11 used to abort with a logical error in HashJoinMethodsImpl.h.
SELECT count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.a < sj_all_left.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT sum(sj_all_left.a) FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.a < sj_all_left.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT sj_all_left.a FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.a < sj_all_left.a) ORDER BY sj_all_left.a; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- The condition may be on the key column itself.
SELECT count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.key < sj_all_left.key); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- The guard is upstream of algorithm selection, so it covers every hash flavour.
SELECT count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.a < sj_all_left.a) SETTINGS join_algorithm = 'parallel_hash'; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.a < sj_all_left.a) SETTINGS join_algorithm = 'grace_hash'; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- Any non-hash algorithm never reaches the Join-engine branch: chooseJoinAlgorithm rejects a
-- mixed condition upstream and keeps its own error. This row is not an assertion about the guard,
-- it anchors the session-level join_algorithm pin above so it cannot be dropped silently.
SELECT count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.a < sj_all_left.a) SETTINGS join_algorithm = 'partial_merge'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 LEFT JOIN sj_all_left AS r ON (t1.key = r.key) AND (t1.a < r.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- Type wrappers: LowCardinality key, Nullable non-key.
SELECT count() FROM t1_lc LEFT JOIN sj_all_left_lc ON (t1_lc.key = sj_all_left_lc.key) AND (t1_lc.a < sj_all_left_lc.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT count() FROM t1 LEFT JOIN sj_all_left_nullable ON (t1.key = sj_all_left_nullable.key) AND (t1.a < sj_all_left_nullable.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- Two mixed conjuncts, either order.
SELECT count() FROM t1 LEFT JOIN sj_all_left_2col ON (t1.key = sj_all_left_2col.key) AND (t1.a < sj_all_left_2col.a) AND (t1.a < sj_all_left_2col.b); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT count() FROM t1 LEFT JOIN sj_all_left_2col ON (t1.key = sj_all_left_2col.key) AND (t1.a < sj_all_left_2col.b) AND (t1.a < sj_all_left_2col.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

-- R12..R15 used to fail with std::bad_variant_access (the engine builds a different maps
-- variant than the query-specific clone prefers).
SELECT count() FROM t1 ANY LEFT JOIN sj_any_left ON (t1.key = sj_any_left.key) AND (t1.a < sj_any_left.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT count() FROM t1 SEMI LEFT JOIN sj_semi_left ON (t1.key = sj_semi_left.key) AND (t1.a < sj_semi_left.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT count() FROM t1 ANTI LEFT JOIN sj_anti_left ON (t1.key = sj_anti_left.key) AND (t1.a < sj_anti_left.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- ANY INNER is a carrier too: the residual is not hoisted for ANY strictness.
SELECT count() FROM t1 ANY INNER JOIN sj_any_inner ON (t1.key = sj_any_inner.key) AND (t1.a < sj_any_inner.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

-- R16/R17: DELIBERATE error code change. These two reported NOT_IMPLEMENTED
-- "StorageJoin with ORs is not supported" before, which was misleading because the queries
-- contain no OR at all. They were already rejected, so no working query changes.
SELECT count() FROM t1 RIGHT JOIN sj_all_right ON (t1.key = sj_all_right.key) AND (t1.a < sj_all_right.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT count() FROM t1 FULL JOIN sj_all_full ON (t1.key = sj_all_full.key) AND (t1.a < sj_all_full.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

SELECT '--- still works: no mixed ON condition ---';

-- ALL INNER hoists the residual to a post-join filter, so it never becomes a mixed
-- expression. The residual is discriminating (only the a = 20 right row qualifies) and the
-- value is asserted against a Memory twin, so an all-true or all-false evaluation is caught.
SELECT 'K1 join', t1.a, sj_all_inner.a FROM t1 INNER JOIN sj_all_inner ON (t1.key = sj_all_inner.key) AND (sj_all_inner.a - t1.a > 15) ORDER BY t1.a, sj_all_inner.a;
SELECT 'K1 oracle', t1.a, mem_right.a FROM t1 INNER JOIN mem_right ON (t1.key = mem_right.key) AND (mem_right.a - t1.a > 15) ORDER BY t1.a, mem_right.a;

SELECT 'K2 equi', count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key);
-- A left-only condition is folded into the join mask, not into a mixed expression.
SELECT 'K3 left-only', count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) AND (t1.a < 3);
-- Not a Join-engine table, so getJoinLocked is never called.
SELECT 'K4 mergetree', count() FROM t1 LEFT JOIN mt_right ON (t1.key = mt_right.key) AND (t1.a < mt_right.a);
SELECT 'K6 joinGet', joinGet('sj_any_left', 'a', 'k1');
SELECT 'K7 any', count() FROM t1 ANY LEFT JOIN sj_any_left ON (t1.key = sj_any_left.key);
SELECT 'K7 semi', count() FROM t1 SEMI LEFT JOIN sj_semi_left ON (t1.key = sj_semi_left.key);
SELECT 'K7 anti', count() FROM t1 ANTI LEFT JOIN sj_anti_left ON (t1.key = sj_anti_left.key);
SELECT 'K9 any inner', count() FROM t1 ANY INNER JOIN sj_any_inner ON (t1.key = sj_any_inner.key);
SELECT 'K10 all inner', count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = sj_all_inner.key) AND (t1.a < sj_all_inner.a);
SELECT 'K11 right equi', count() FROM t1 RIGHT JOIN sj_all_right ON (t1.key = sj_all_right.key);

-- ALL INNER applies a right-only conjunct after the join, so it never becomes a filter over the
-- prebuilt join. The cases differ in how the conjunct arises: optimize_and_compare_chain derives
-- it in K12, the rest spell it out in the query.
SELECT 'K12 derived', count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = 'k1') AND (t1.key = sj_all_inner.key) SETTINGS optimize_and_compare_chain = 1;
-- K12 only carries a derived conjunct while that pass is on, so pin the count either way. Both
-- arms state the setting explicitly because the test runner randomizes it.
SELECT 'K12 equals nodes on', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = 'k1') AND (t1.key = sj_all_inner.key) SETTINGS optimize_and_compare_chain = 1) WHERE explain ILIKE '%function_name: equals%';
SELECT 'K12 equals nodes off', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = 'k1') AND (t1.key = sj_all_inner.key) SETTINGS optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: equals%';
SELECT 'K13 right-only key', count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = sj_all_inner.key) AND (sj_all_inner.key = 'k1');
-- The miss direction: no right row has key 'k2', so an applied predicate yields nothing while an
-- ignored one yields all six pairs. optimize_and_compare_chain is pinned off so the mirror-image
-- left predicate is not derived, which would filter the left side to nothing and produce the same
-- zero for the wrong reason.
SELECT 'K13 right-only key miss', count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = sj_all_inner.key) AND (sj_all_inner.key = 'k2') SETTINGS optimize_and_compare_chain = 0;
-- A LowCardinality key makes the condition LowCardinality(UInt8), which does not take
-- toBoolIfNeeded's UInt8 early return and so reaches the join as a computed column.
SELECT 'K17 lc key', count() FROM t1_lc INNER JOIN sj_all_inner_lc ON (t1_lc.key = sj_all_inner_lc.key) AND (sj_all_inner_lc.key = 'k1');
SELECT 'K17 lc key miss', count() FROM t1_lc INNER JOIN sj_all_inner_lc ON (t1_lc.key = sj_all_inner_lc.key) AND (sj_all_inner_lc.key = 'k2') SETTINGS optimize_and_compare_chain = 0;
-- Discriminating (only the a = 20 right row qualifies), asserted against the Memory twin.
SELECT 'K14 join', t1.a, sj_all_inner.a FROM t1 INNER JOIN sj_all_inner ON (t1.key = sj_all_inner.key) AND (sj_all_inner.a > 15) ORDER BY t1.a, sj_all_inner.a;
SELECT 'K14 oracle', t1.a, mem_right.a FROM t1 INNER JOIN mem_right ON (t1.key = mem_right.key) AND (mem_right.a > 15) ORDER BY t1.a, mem_right.a;
-- The equi-only control keeps the plan assertion sensitive to the filter position.
SELECT 'K15 post-join', count() > 0 FROM (EXPLAIN SELECT count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = sj_all_inner.key) AND (sj_all_inner.a > 15)) WHERE explain ILIKE '%Filter (Post Join Actions)%';
SELECT 'K15 control', count() > 0 FROM (EXPLAIN SELECT count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = sj_all_inner.key)) WHERE explain ILIKE '%Filter (Post Join Actions)%';
-- A UInt8 or Nullable(UInt8) column used directly as a condition stays a stored column instead of
-- becoming a computed one, so it reaches the join by a different route than K12 to K14. The two
-- directions select complementary rows, which catches a mask that is all-true or all-false.
SELECT 'K16 flag', t1.a, sj_bool.a FROM t1 INNER JOIN sj_bool ON (t1.key = sj_bool.key) AND sj_bool.flag ORDER BY t1.a, sj_bool.a;
SELECT 'K16 not flag', t1.a, sj_bool.a FROM t1 INNER JOIN sj_bool ON (t1.key = sj_bool.key) AND NOT sj_bool.flag ORDER BY t1.a, sj_bool.a;
SELECT 'K16 nullable flag', t1.a, sj_bool_nullable.a FROM t1 INNER JOIN sj_bool_nullable ON (t1.key = sj_bool_nullable.key) AND sj_bool_nullable.flag ORDER BY t1.a, sj_bool_nullable.a;

SELECT '--- other rejections keep their own error, the guard did not widen ---';

-- Strictness mismatch against the engine declaration.
SELECT count() FROM t1 SEMI LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- A genuinely disjunctive ON is rejected earlier and independently, so centralizing the
-- RIGHT/FULL rejection above did not swallow the OR path.
SELECT count() FROM t1 LEFT JOIN sj_all_left ON (t1.key = sj_all_left.key) OR (t1.a = sj_all_left.a); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- An outer join evaluates its ON conditions during the join (non-matching rows are NULL-extended
-- rather than dropped), so a right-only conjunct cannot move above the join the way K13 to K16 do.
SELECT count() FROM t1 LEFT JOIN sj_all_left_2col ON (t1.key = sj_all_left_2col.key) AND (sj_all_left_2col.b > 150); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
-- A derived join key is not a stored column of the engine, whichever side it is on.
SELECT count() FROM t1 INNER JOIN sj_all_inner ON (t1.key = toString(sj_all_inner.key)); -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

DROP TABLE t1;
DROP TABLE t1_lc;
DROP TABLE sj_all_left;
DROP TABLE sj_all_left_2col;
DROP TABLE sj_all_left_nullable;
DROP TABLE sj_all_left_lc;
DROP TABLE sj_any_left;
DROP TABLE sj_semi_left;
DROP TABLE sj_anti_left;
DROP TABLE sj_any_inner;
DROP TABLE sj_all_inner;
DROP TABLE sj_all_inner_lc;
DROP TABLE sj_all_right;
DROP TABLE sj_all_full;
DROP TABLE mt_right;
DROP TABLE mem_right;
DROP TABLE sj_bool;
DROP TABLE sj_bool_nullable;
