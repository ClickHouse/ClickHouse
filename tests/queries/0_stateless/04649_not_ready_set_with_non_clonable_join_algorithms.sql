-- Regression test pinning the *boundary* of `JoinStep::clone`.
--
-- `JoinStep::clone` deep-clones the underlying `IJoin`, and it can only do so for the algorithms
-- that implement `IJoin::clone`: `HashJoin`, `ConcurrentHashJoin`, `ConstantJoin` and
-- `FullSortingMergeJoin`. `JoinSwitcher` (`join_algorithm = 'auto'`), `SpillingHashJoin`,
-- `MergeJoin` / `PartialMergeJoin` and `GraceHashJoin` inherit `IJoin::isCloneSupported() == false`,
-- so `JoinStep::clone` throws `NOT_IMPLEMENTED` for them.
--
-- That `NOT_IMPLEMENTED` must never reach the user: `FutureSetFromSubquery::buildOrderedSetInplace`
-- has to catch it and take the destructive fallback (consume `source` directly), which is exactly
-- what happened for *every* join shape before the clone path was introduced. This test checks that
-- an `IN` subquery whose source plan contains a join with a non-clonable algorithm still returns the
-- correct result, i.e. the narrowed clone contract degrades to the previous behavior instead of
-- failing the query.
--
-- The complementary test `04550_not_ready_set_with_join_subquery` covers the clone-supported
-- algorithms, where the preserved source additionally makes a silent in-place build failure
-- recoverable.

DROP TABLE IF EXISTS t_outer_nc;
DROP TABLE IF EXISTS t_small_nc;
DROP TABLE IF EXISTS t_big_nc;

CREATE TABLE t_outer_nc (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_outer_nc SELECT number FROM numbers(1000);

CREATE TABLE t_small_nc (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_small_nc SELECT number FROM numbers(10);
CREATE TABLE t_big_nc (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_big_nc SELECT number FROM numbers(1000);

SET use_index_for_in_with_subqueries = 1;
SET enable_analyzer = 1;

-- `partial_merge` -> `MergeJoin`, not clonable.
SET join_algorithm = 'partial_merge';
SELECT count() FROM t_outer_nc WHERE k IN (SELECT s.k FROM t_small_nc AS s INNER JOIN t_big_nc AS b ON s.k = b.k);

-- `grace_hash` -> `GraceHashJoin`, not clonable.
SET join_algorithm = 'grace_hash';
SELECT count() FROM t_outer_nc WHERE k IN (SELECT s.k FROM t_small_nc AS s INNER JOIN t_big_nc AS b ON s.k = b.k);

-- `auto` -> `JoinSwitcher`, not clonable.
SET join_algorithm = 'auto';
SELECT count() FROM t_outer_nc WHERE k IN (SELECT s.k FROM t_small_nc AS s INNER JOIN t_big_nc AS b ON s.k = b.k);

DROP TABLE t_outer_nc;
DROP TABLE t_small_nc;
DROP TABLE t_big_nc;
