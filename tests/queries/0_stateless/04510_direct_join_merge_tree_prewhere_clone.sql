-- Tags: no-parallel-replicas
-- Regression test for crashes in a direct JOIN over a MergeTree right table, see PR #109932.
-- Requires several concurrent lookups, so the left table has many blocks and max_threads is pinned.

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS attributes;
DROP TABLE IF EXISTS allowed_attrs;
DROP TABLE IF EXISTS attributes_rp;
DROP ROW POLICY IF EXISTS rp ON attributes_rp;

CREATE TABLE events (`Id` UInt64) ENGINE = Memory;
-- Many small blocks in the left table so the direct join performs multiple concurrent lookups.
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 10;
INSERT INTO events SELECT number FROM numbers(320);
SET max_block_size = DEFAULT;

CREATE TABLE attributes
(
    `EventId` UInt64,
    `Attribute` String
)
ENGINE = MergeTree
ORDER BY EventId;

INSERT INTO attributes SELECT number AS EventId, concat('Attribute_', toString(number)) AS Attribute FROM numbers(320);

SET enable_analyzer = 1;
SET join_algorithm = 'direct';
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

-- PREWHERE on the direct-join right table, with column pruning enabled.
SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id PREWHERE t1.Attribute != ''
SETTINGS query_plan_remove_unused_columns = 1, max_threads = 4, enable_shared_storage_snapshot_in_query = 0;

-- Right-table PREWHERE with IN(subquery).
CREATE TABLE allowed_attrs (`a` String) ENGINE = Memory;
INSERT INTO allowed_attrs SELECT concat('Attribute_', toString(number)) FROM numbers(160);

SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id
PREWHERE t1.Attribute IN (SELECT a FROM allowed_attrs)
SETTINGS query_plan_remove_unused_columns = 1, max_threads = 4;

-- Row policy on the direct-join right table. Uses a dedicated table so the policy does not
-- affect the queries above.
CREATE TABLE attributes_rp
(
    `EventId` UInt64,
    `Attribute` String
)
ENGINE = MergeTree
ORDER BY EventId;

INSERT INTO attributes_rp SELECT number AS EventId, concat('Attribute_', toString(number)) AS Attribute FROM numbers(320);

CREATE ROW POLICY rp ON attributes_rp USING EventId < 100 AS PERMISSIVE TO ALL;

SELECT count(), countIf(t1.Attribute != '')
FROM events AS t0 INNER JOIN attributes_rp AS t1 ON t1.EventId = t0.Id
SETTINGS query_plan_remove_unused_columns = 1, max_threads = 4;

DROP ROW POLICY rp ON attributes_rp;

DROP TABLE events;
DROP TABLE attributes;
DROP TABLE allowed_attrs;
DROP TABLE attributes_rp;
