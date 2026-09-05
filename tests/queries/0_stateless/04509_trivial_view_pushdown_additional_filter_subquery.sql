-- A view-keyed additional_table_filters predicate containing a subquery (either
-- a standalone one, like a ROW POLICY `USING (SELECT ...)`, or one embedded as
-- an operand) must suppress the trivial-view pushdown, the same way a
-- subquery in the outer query does (see astContainsSubquery). On the normal
-- StorageView path this subquery runs once on the coordinator; the pushdown
-- would instead evaluate it once per shard, which can change results or throw
-- if it reads an initiator-local table.
--
-- additional_table_filters is parsed via ParserExpression, whose ParserSubquery
-- component always wraps a parenthesized SELECT in ASTSubquery regardless of
-- position (top-level standalone filter or embedded as an operand), so
-- astContainsSubquery's existing ASTSubquery check already covers both shapes.
-- This test locks that in explicitly.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04509_local;
DROP TABLE IF EXISTS 04509_dist;
DROP VIEW IF EXISTS 04509_view;
DROP TABLE IF EXISTS 04509_local_filter;

CREATE TABLE 04509_local (id UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04509_dist AS 04509_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04509_local);

INSERT INTO 04509_dist VALUES (1), (2), (3);
SYSTEM FLUSH DISTRIBUTED 04509_dist;

CREATE VIEW 04509_view AS SELECT id FROM 04509_dist;

-- Initiator-local table: not part of the Distributed cluster, so if the
-- subquery were pushed down and evaluated per-shard it would read an empty
-- (or, on a real multi-shard cluster, inconsistent) table instead of the
-- single row visible on the coordinator.
CREATE TABLE 04509_local_filter (id UInt32) ENGINE = Memory;
INSERT INTO 04509_local_filter VALUES (2);

SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Baseline: pushdown fires for a view-keyed additional filter with no subquery.
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id FROM 04509_view
      SETTINGS additional_table_filters = {'04509_view': 'id > 0'});

-- Standalone subquery as the entire filter (like ROW POLICY USING (SELECT ...)):
-- pushdown must be suppressed (plan keeps the "VIEW subquery" steps) ...
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04509_view
      SETTINGS additional_table_filters = {'04509_view': '(SELECT id FROM 04509_local_filter LIMIT 1)'});

-- ... and the result must be correct and identical with the setting on and off.
SELECT id FROM 04509_view
SETTINGS additional_table_filters = {'04509_view': '(SELECT id FROM 04509_local_filter LIMIT 1)'};
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM 04509_view
SETTINGS additional_table_filters = {'04509_view': '(SELECT id FROM 04509_local_filter LIMIT 1)'};
SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Subquery embedded as an operand: likewise suppressed and correct.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04509_view
      SETTINGS additional_table_filters = {'04509_view': 'id = (SELECT id FROM 04509_local_filter LIMIT 1)'});

SELECT id FROM 04509_view
SETTINGS additional_table_filters = {'04509_view': 'id = (SELECT id FROM 04509_local_filter LIMIT 1)'};
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM 04509_view
SETTINGS additional_table_filters = {'04509_view': 'id = (SELECT id FROM 04509_local_filter LIMIT 1)'};
SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP TABLE 04509_local_filter;
DROP VIEW 04509_view;
DROP TABLE 04509_dist;
DROP TABLE 04509_local;
