-- Tags: no-old-analyzer
-- The fix lives in the new analyzer (`createUniqueAliasesIfNecessary`); the old
-- analyzer rejects the AST-fuzzer query shape with `UNKNOWN_IDENTIFIER` before
-- reaching the planner, so the bug cannot manifest there.
--
-- Regression test for the chronic AST-fuzzer / serverfuzz exception
--   Logical error: 'Column identifier dummy is already registered'
-- (STID 4697-4326, issue ClickHouse/ClickHouse#104877).
--
-- An ALTER UPDATE whose WHERE clause contains an EXISTS over a subquery that
-- reads from a real source table triggered the planner's mutations path
-- (buildSubqueryPlansForSetsAndAdd) to plan a subquery whose source nodes had
-- no __tableN aliases. prepareBuildQueryPlanForTableExpression registered the
-- bare column name for the wrapping QueryNode, and CollectSourceColumnsVisitor
-- then tried to register the same bare name for the inner TableNode, throwing
-- the exception.
--
-- The merged fix family for the same error message (#100770, #101048, #101051,
-- #101104) addressed UNION ALL / additional_result_filter paths but did not
-- cover the mutations + EXISTS pattern. Reproduces deterministically against
-- master HEAD without the fix.

DROP TABLE IF EXISTS t_04303;
CREATE TABLE t_04303 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04303 VALUES (1, 10), (2, 20);

-- Minimal reproducer: EXISTS over an unaliased real-table subquery.
ALTER TABLE t_04303 UPDATE v = 0 WHERE exists((SELECT * FROM system.one)) SETTINGS mutations_sync = 2;
SELECT id, v FROM t_04303 ORDER BY id;

-- Pattern from the AST fuzzer hit on master SHA 3120048c80b0 (2026-05-27):
--   ALTER TABLE test_updates (UPDATE dynamic = NULL
--     WHERE exists((SELECT DISTINCT *, currentDatabase() > database)));
DROP TABLE IF EXISTS test_updates_04303;
CREATE TABLE test_updates_04303 (id UInt64, dynamic Dynamic)
    ENGINE = MergeTree ORDER BY id;
INSERT INTO test_updates_04303 VALUES (1, 'a'), (2, 'b');
ALTER TABLE test_updates_04303
    (UPDATE dynamic = NULL WHERE exists((SELECT DISTINCT *, currentDatabase() > database)))
    SETTINGS mutations_sync = 2;
SELECT id, isNull(dynamic) FROM test_updates_04303 ORDER BY id;

-- A NOT EXISTS variant, which the analyzer rewrites to `not exists((subq))`
-- and then expands the same way.
DROP TABLE IF EXISTS t_04303_b;
CREATE TABLE t_04303_b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04303_b VALUES (1, 10), (2, 20);
ALTER TABLE t_04303_b UPDATE v = 1
    WHERE NOT exists((SELECT * FROM system.numbers WHERE number > 1000000 LIMIT 1))
    SETTINGS mutations_sync = 2;
SELECT id, v FROM t_04303_b ORDER BY id;

DROP TABLE t_04303;
DROP TABLE test_updates_04303;
DROP TABLE t_04303_b;
