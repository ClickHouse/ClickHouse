-- Companion to 04507_trivial_view_pushdown_virtual_columns: `_table`/`_database`
-- must also suppress the trivial-view pushdown when referenced only through a
-- view-keyed `additional_table_filters` entry, not just when referenced
-- directly in the outer query's projection/WHERE. `additional_table_filters`
-- is folded into the outer WHERE before the view body is inlined (see the
-- comment on that merge in PlannerJoinTree.cpp), so a filter that reads a view
-- virtual column is subject to the same StorageWithCommonVirtualColumns::read
-- hazard as a directly-referenced one.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04508_local;
DROP TABLE IF EXISTS 04508_dist;
DROP VIEW IF EXISTS 04508_view;

CREATE TABLE 04508_local (id UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04508_dist AS 04508_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04508_local);

INSERT INTO 04508_dist VALUES (1), (2), (3);
SYSTEM FLUSH DISTRIBUTED 04508_dist;

CREATE VIEW 04508_view AS SELECT id FROM 04508_dist;

SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Baseline: pushdown fires for a view-keyed additional filter that does not
-- reference `_table`/`_database`.
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id FROM 04508_view
      SETTINGS additional_table_filters = {'04508_view': 'id > 0'});

-- `_table` in a view-keyed additional filter: pushdown must be suppressed
-- (plan keeps the "VIEW subquery" steps) ...
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04508_view
      SETTINGS additional_table_filters = {'04508_view': '_table = ''04508_view'''});

-- ... and the filter must correctly match the view's own name, identical with
-- the setting on and off.
SELECT id FROM 04508_view ORDER BY id
SETTINGS additional_table_filters = {'04508_view': '_table = ''04508_view'''};
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM 04508_view ORDER BY id
SETTINGS additional_table_filters = {'04508_view': '_table = ''04508_view'''};
SET optimize_trivial_view_pushdown_to_distributed = 1;

-- `_database` in a view-keyed additional filter: likewise suppressed. The
-- filter matches any non-empty database name so the assertion is stable
-- regardless of which database the test runs in.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04508_view
      SETTINGS additional_table_filters = {'04508_view': 'notEmpty(_database)'});

SELECT id FROM 04508_view ORDER BY id
SETTINGS additional_table_filters = {'04508_view': 'notEmpty(_database)'};
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM 04508_view ORDER BY id
SETTINGS additional_table_filters = {'04508_view': 'notEmpty(_database)'};
SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP VIEW 04508_view;
DROP TABLE 04508_dist;
DROP TABLE 04508_local;
