SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
-- The primary-key arms reach buildOrderedSetInplace only while this is on. It is the default,
-- pinned so a future default or randomization change cannot silently retire them.
SET use_index_for_in_with_subqueries = 1;
-- Same reason, and this one is randomized 0/1 by the test runner: at 0 the Distributed read goes
-- to a remote replica instead of an in-process shard-local plan, so every arm below passes even
-- without the fix. Both values are correct behaviour; only 1 exercises the gate.
SET prefer_localhost_replica = 1;

DROP TABLE IF EXISTS t_04712 SYNC;
CREATE TABLE t_04712 (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO t_04712 VALUES (1), (2), (3);
DROP TABLE IF EXISTS dist_04712 SYNC;
CREATE TABLE dist_04712 AS t_04712 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_04712);

-- A materialized CTE reading a Distributed table and filtered by IN (another materialized CTE),
-- referenced twice. The plain-MergeTree twin is
-- 04227_materialized_cte_reused_with_in_subquery, which already passes.
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a, rs AS b;

-- The index path is actually taken: assert the in-place set reached KeyCondition. Two oracles,
-- because the arms above and the arms below take different routes: this one covers the local
-- plain-MergeTree route, the next one the shard-local route under Distributed. Both return 0
-- if buildOrderedSetInplace stops being reached, which is what a pinned setting cannot detect.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    WITH ct AS MATERIALIZED (SELECT 1 AS c),
         rs AS MATERIALIZED (SELECT * FROM t_04712 WHERE c IN (SELECT c FROM ct))
    SELECT count() FROM rs AS a, rs AS b
) WHERE explain ILIKE '%Condition:%c in%set)%';

-- Same assertion for the reproducer's own Distributed shape. serialize_query_plan is pinned
-- per statement, not per file, so the arms above keep exercising both of its values.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1, distributed = 1
    WITH ct AS MATERIALIZED (SELECT 1 AS c),
         rs AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM ct))
    SELECT count() FROM rs AS a, rs AS b
) WHERE explain ILIKE '%Condition:%c in%set)%'
SETTINGS serialize_query_plan = 0;

WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a ANY LEFT JOIN rs AS b USING c;

WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM (SELECT * FROM rs UNION ALL SELECT * FROM rs);

-- Non-primary-key variant: routes through buildSetInplace (VirtualColumnUtils) rather than
-- buildOrderedSetInplace (KeyCondition). Mirrors 04227's t2_04227 case.
DROP TABLE IF EXISTS t2_04712 SYNC;
CREATE TABLE t2_04712 (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t2_04712 VALUES (1, 10), (2, 20), (3, 30);
DROP TABLE IF EXISTS dist2_04712 SYNC;
CREATE TABLE dist2_04712 AS t2_04712 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t2_04712);

WITH ct AS MATERIALIZED (SELECT b FROM t2_04712 LIMIT 10),
     rs AS MATERIALIZED (SELECT * FROM dist2_04712 WHERE b IN (SELECT b FROM ct))
SELECT count() FROM rs AS x ANY LEFT JOIN rs AS y USING a;

-- One more level of nesting: the ct reader sits inside a set source held by a nested
-- DelayedCreatingSetsStep, which getChildPlans() does not expose.
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM t_04712 WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b;

-- Three chained levels: pins that a level-N CTE is still materialized before the level-N-1 CTE
-- that depends on it, i.e. that the delayed steps are attached in forward level order.
WITH c1 AS MATERIALIZED (SELECT 1 AS c),
     c2 AS MATERIALIZED (SELECT c FROM t_04712 WHERE c IN (SELECT c FROM c1)),
     rs AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM c2))
SELECT count() FROM rs AS a, rs AS b;

-- Two CTEs at one level plus a third below them.
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs1 AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM ct)),
     rs2 AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs1 AS a, rs1 AS b, rs2 AS d, rs2 AS e;

-- The deeper CTE is also read directly in the main query. Ownership of its materializing step
-- moves into the nested plans, so this pins that the outer reader of ct stays gated by whichever
-- step wins the claim.
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_04712 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a, rs AS b, ct AS d;

-- A table-function remote source, not only the Distributed-engine one.
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM cluster('test_shard_localhost', currentDatabase(), t_04712)
                         WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a ANY LEFT JOIN rs AS b USING (c);

-- The other in-place entry point: with the ordered build declined, PREWHERE falls back to
-- buildSetInplace, which assembles its own standalone pipeline the same way.
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_04712 PREWHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a, rs AS b
SETTINGS use_index_for_in_with_subqueries = 0;

-- Control: the plain-MergeTree counterpart passes before this fix too and must not regress.
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM t_04712 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a, rs AS b;

-- Control: force_primary_key with an in-place set over a materialized CTE must keep using the
-- index (the 03928 shape). Nothing here declines a set build, so this stays green.
WITH ct AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM t_04712 WHERE c IN (SELECT c FROM ct WHERE c > 1) OR c IN (SELECT c FROM ct WHERE c > 1)
SETTINGS force_primary_key = 1;

DROP TABLE dist2_04712;
DROP TABLE t2_04712;
DROP TABLE dist_04712;
DROP TABLE t_04712;
