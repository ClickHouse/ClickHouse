-- Tags: no-parallel-replicas
-- Regression test for a crash in direct JOIN over a MergeTree right table with PREWHERE.
-- The pushed-down filter becomes a shared PrewhereInfo on the MergeTree lookup plan, which
-- DirectJoinMergeTreeEntity::getByKeys clones and re-optimizes on every lookup (across
-- max_threads threads). Column pruning (query_plan_remove_unused_columns) mutated the shared
-- PrewhereInfo/row_level_filter DAGs in place, corrupting them across clones. See PR #109932.
-- This is a heap-use-after-free, so it aborts deterministically only under a sanitizer (ASan);
-- on a plain build the corrupted read usually returns silently. CI runs it under asan/ubsan.

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS attributes;
DROP TABLE IF EXISTS allowed_attrs;

CREATE TABLE events (`Id` UInt64) ENGINE = Memory;
-- Several blocks in the left table so the direct join performs multiple lookups.
INSERT INTO events SELECT number FROM numbers(500);
INSERT INTO events SELECT number FROM numbers(500, 500);

CREATE TABLE attributes
(
    `EventId` UInt64,
    `Attribute` String
)
ENGINE = MergeTree
ORDER BY EventId;

INSERT INTO attributes SELECT number AS EventId, concat('Attribute_', toString(number)) AS Attribute FROM numbers(1000);

SET enable_analyzer = 1;
SET join_algorithm = 'direct';
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

-- PREWHERE on the direct-join right table with column pruning enabled: must not corrupt the
-- shared PrewhereInfo across per-lookup clones (previously crashed in pruneFilterDAGOutputsByPosition).
SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id PREWHERE t1.Attribute != ''
SETTINGS query_plan_remove_unused_columns = 1;

-- Right-table PREWHERE with IN(subquery): the subquery set is registered in the outer query's
-- PreparedSets and built once up-front by CreatingSets, so the shared FutureSetFromSubquery is
-- already created before any getByKeys lookup clones the plan. Verifies no "Not-ready Set" path.
CREATE TABLE allowed_attrs (`a` String) ENGINE = Memory;
INSERT INTO allowed_attrs SELECT concat('Attribute_', toString(number)) FROM numbers(500);

SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id
PREWHERE t1.Attribute IN (SELECT a FROM allowed_attrs)
SETTINGS query_plan_remove_unused_columns = 1;

-- Row policy on the direct-join right table: the policy filter becomes a shared row_level_filter
-- on the lookup plan (the other carrier the fix deep-clones). Column pruning must not corrupt it
-- across per-lookup clones. Uses a dedicated table so the policy does not affect the queries above.
CREATE TABLE attributes_rp
(
    `EventId` UInt64,
    `Attribute` String
)
ENGINE = MergeTree
ORDER BY EventId;

INSERT INTO attributes_rp SELECT number AS EventId, concat('Attribute_', toString(number)) AS Attribute FROM numbers(1000);

CREATE ROW POLICY rp ON attributes_rp USING EventId < 300 AS PERMISSIVE TO ALL;

SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes_rp AS t1 ON t1.EventId = t0.Id
SETTINGS query_plan_remove_unused_columns = 1;

DROP ROW POLICY rp ON attributes_rp;

-- Shared StorageSnapshot facet (STID 3942-460f): getByKeys clones the lookup plan per pipeline
-- thread, all sharing the ReadFromMergeTree's storage_snapshot. With the parts-from-snapshot strip
-- armed (enable_shared_storage_snapshot_in_query = 0), initializePipeline resets storage_snapshot->data
-- in place; concurrent clones sharing one snapshot double-freed it. clone() now gives each stripping
-- clone its own StorageSnapshot. Runs the direct join across many threads with the strip armed.
SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id
SETTINGS enable_shared_storage_snapshot_in_query = 0, max_threads = 16;

DROP TABLE events;
DROP TABLE attributes;
DROP TABLE allowed_attrs;
DROP TABLE attributes_rp;
