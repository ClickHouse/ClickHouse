-- Tags: no-parallel-replicas

-- An empty right side through the order-preserving spill path: the spill threshold must not
-- change the result of a valid join against an empty table. An empty build stays in memory by
-- design - every caller of `switchToExternalJoin` skips it (nothing to spill; a switch would
-- only allocate grace buckets or a disk writer): the threshold net requires inserted rows and
-- the scheduler-driven `SpillingHashJoin::spill` returns without switching. The external
-- `MergeJoin` fallback therefore always receives at least one non-empty right block, which a
-- never-fed `SortedBlocksWriter` could not finalize ("Can't initialize pipeline with empty
-- pipe").

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 1, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS er_events;
DROP TABLE IF EXISTS er_payloads;

CREATE TABLE er_events (t UInt32, id UInt32) ENGINE = MergeTree ORDER BY t;
INSERT INTO er_events SELECT number, number FROM numbers(100);

CREATE TABLE er_payloads (id UInt32, payload String) ENGINE = MergeTree ORDER BY tuple();

-- The empty right table reads through ReadNothing, which prints no ReadType line.
SELECT '-- The ordered plan engages over the empty right side';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT er_events.t, er_payloads.payload FROM er_events
    LEFT JOIN er_payloads ON er_events.id = er_payloads.id
    ORDER BY er_events.t LIMIT 3
    SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%ReadType%';

SELECT '-- LEFT ALL: every left row with defaults, in the left order';

SELECT count(), countIf(payload = ''), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT er_events.t AS t, er_payloads.payload AS payload FROM er_events
    LEFT JOIN er_payloads ON er_events.id = er_payloads.id
    ORDER BY er_events.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

SELECT '-- LEFT ANY: the same';

SELECT count(), countIf(payload = ''), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT er_events.t AS t, er_payloads.payload AS payload FROM er_events
    LEFT ANY JOIN er_payloads ON er_events.id = er_payloads.id
    ORDER BY er_events.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

SELECT '-- INNER: no rows';

SELECT count() FROM (
    SELECT er_events.t AS t FROM er_events
    INNER JOIN er_payloads ON er_events.id = er_payloads.id
    ORDER BY er_events.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

SELECT '-- Concurrent build over the empty right side';

SELECT count(), countIf(payload = ''), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT er_events.t AS t, er_payloads.payload AS payload FROM er_events
    LEFT JOIN er_payloads ON er_events.id = er_payloads.id
    ORDER BY er_events.t
    SETTINGS join_algorithm = 'parallel_hash', max_threads = 8
);

SELECT '-- Identical to the non-spill-capable hash join';

SELECT
    (SELECT sum(cityHash64(t, payload = '')) FROM (
        SELECT er_events.t AS t, er_payloads.payload AS payload FROM er_events
        LEFT JOIN er_payloads ON er_events.id = er_payloads.id
        ORDER BY er_events.t
        SETTINGS join_algorithm = 'hash'
    ))
    =
    (SELECT sum(cityHash64(t, payload = '')) FROM (
        SELECT er_events.t AS t, er_payloads.payload AS payload FROM er_events
        LEFT JOIN er_payloads ON er_events.id = er_payloads.id
        ORDER BY er_events.t
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ));

DROP TABLE er_events;
DROP TABLE er_payloads;
