-- Tags: no-parallel-replicas
-- Regression test for crashes in a direct JOIN over a MergeTree right table, see PR #109932.
-- The crash is only observable under a sanitizer build; on a plain build the test passes.

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

-- PREWHERE on the direct-join right table, with column pruning enabled.
SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id PREWHERE t1.Attribute != ''
SETTINGS query_plan_remove_unused_columns = 1;

-- Right-table PREWHERE with IN(subquery).
CREATE TABLE allowed_attrs (`a` String) ENGINE = Memory;
INSERT INTO allowed_attrs SELECT concat('Attribute_', toString(number)) FROM numbers(500);

SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id
PREWHERE t1.Attribute IN (SELECT a FROM allowed_attrs)
SETTINGS query_plan_remove_unused_columns = 1;

-- Row policy on the direct-join right table. Uses a dedicated table so the policy does not
-- affect the queries above.
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

-- Direct join across many threads without a query-wide shared storage snapshot.
SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id
SETTINGS enable_shared_storage_snapshot_in_query = 0, max_threads = 16;

DROP TABLE events;
DROP TABLE attributes;
DROP TABLE allowed_attrs;
DROP TABLE attributes_rp;
