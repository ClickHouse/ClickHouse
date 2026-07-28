-- Tests for issue #100422: a correlated subquery (e.g. `exists((SELECT ...))`) inside a filter that an
-- optimization clones or distributes was copied into several copies sharing one action name.
-- Decorrelation then added the same synthetic column on both sides of the generated join and
-- `HashJoin::getNonJoinedBlocks` failed with `Unexpected number of columns in result sample block`.
-- Two avenues produce such a copy: `convert_query_to_cnf` distributes the filter over AND, and
-- `optimize_and_compare_chain` derives transitive comparisons that clone a compared operand.
-- All queries below must run without a logical error and return the same result as without the
-- optimizations. Correlated subqueries require the analyzer, so keep it enabled.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET convert_query_to_cnf = 1;
SET optimize_and_compare_chain = 1;

DROP TABLE IF EXISTS t_04540;
CREATE TABLE t_04540 (a UInt32) ENGINE = Memory;
INSERT INTO t_04540 VALUES (0), (1), (2);

-- Minimal reproducer: correlated `exists` OR'd with a conjunction and further disjuncts.
SELECT * FROM t_04540
WHERE exists((SELECT a <= 100)) OR (a >= 0 AND a <= 50 AND a > 10) OR (2 != a) OR (a = 99)
ORDER BY a;

-- QUALIFY variant.
DROP TABLE IF EXISTS t2_04540;
CREATE TABLE t2_04540 (g UInt8, h Nullable(UInt16)) ENGINE = Memory;
INSERT INTO t2_04540 VALUES (0, 0), (1, 1);
SELECT * FROM t2_04540 QUALIFY and(g >= 0, g < exists((SELECT h))) ORDER BY g;

-- GLOBAL IN combined with correlated `exists`.
SELECT number FROM numbers(3)
WHERE number GLOBAL IN (SELECT 1) AND number >= exists(SELECT 1 WHERE number = 0)
ORDER BY number;

-- WHERE AND-compare-chain variant.
SELECT number FROM numbers(1) WHERE number >= exists(SELECT 1 WHERE number = 0) AND number = 0;

DROP TABLE t_04540;
DROP TABLE t2_04540;
