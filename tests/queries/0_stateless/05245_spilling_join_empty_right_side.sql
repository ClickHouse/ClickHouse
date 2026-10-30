-- Tags: no-parallel-replicas

-- An empty right side through the order-preserving spill path: the spill threshold must not
-- change the result of a valid join against an empty table. The threshold checks compare
-- bytes only, so a build whose hash tables preallocate their buffers (a fixed map for a
-- UInt8 key) can switch to the external join without a single stored row; the external
-- `MergeJoin` and its never-fed `SortedBlocksWriter` must finalize to zero spilled blocks
-- instead of failing with "Can't initialize pipeline with empty pipe".

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

SELECT '-- A UInt8 key preallocates the fixed hash map: the empty build switches to MergeJoin';

-- A fixed map allocates its whole buffer up front, so the byte threshold trips with zero stored
-- rows and the pinned external MergeJoin finalizes a never-fed disk writer.
DROP TABLE IF EXISTS er_flags;
CREATE TABLE er_flags (id UInt8, payload String) ENGINE = MergeTree ORDER BY tuple();

SELECT count(), countIf(payload = ''), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT er_events.t AS t, er_flags.payload AS payload FROM er_events
    LEFT JOIN er_flags ON toUInt8(er_events.id) = er_flags.id
    ORDER BY er_events.t
)
SETTINGS join_algorithm = 'hash', max_threads = 1, log_comment = '05245_empty_switch_merge';

-- Correctness only (the INNER in-order pin depends on the virtual-row bounds, so the empty
-- build may switch to either external join): zero rows either way.
SELECT count() FROM (
    SELECT er_events.t AS t FROM er_events
    INNER JOIN er_flags ON toUInt8(er_events.id) = er_flags.id
    ORDER BY er_events.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

SELECT '-- An all-filtered FULL ALL build has stored rows but no hash keys: it still spills';

-- Every right row fails the ON condition, so no key reaches the hash table while the blocks
-- stay stored for the unmatched-right output. The byte threshold must still switch the build
-- to GraceHashJoin instead of retaining the whole right side in memory.
DROP TABLE IF EXISTS fa_left;
DROP TABLE IF EXISTS fa_right;
CREATE TABLE fa_left (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO fa_left SELECT number, number + 1 FROM numbers(1000);
CREATE TABLE fa_right (k UInt32, flag UInt8, payload String) ENGINE = MergeTree ORDER BY k;
INSERT INTO fa_right SELECT number, 0, repeat('x', 100) FROM numbers(20000);

SELECT count(), countIf(v > 0), countIf(payload != ''), countIf(v > 0 AND payload != '') FROM (
    SELECT fa_left.v AS v, fa_right.payload AS payload FROM fa_left
    FULL JOIN fa_right ON fa_left.k = fa_right.k AND fa_right.flag = 1
)
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 1000000,
         log_comment = '05245_full_all_filtered_hash';

SELECT count(), countIf(v > 0), countIf(payload != ''), countIf(v > 0 AND payload != '') FROM (
    SELECT fa_left.v AS v, fa_right.payload AS payload FROM fa_left
    FULL JOIN fa_right ON fa_left.k = fa_right.k AND fa_right.flag = 1
)
SETTINGS join_algorithm = 'parallel_hash', max_threads = 8, max_bytes_before_external_join = 1000000,
         log_comment = '05245_full_all_filtered_parallel_hash';

SELECT '-- The switches happened';

SYSTEM FLUSH LOGS system.query_log;

SELECT
    log_comment,
    if(ProfileEvents['JoinSpillingHashJoinSwitchedToMergeJoin'] >= 1, 'OK',
       format('Fail: no switch to MergeJoin, query_id="{}"', query_id))
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_date >= yesterday()
    AND log_comment LIKE '05245_empty_switch_merge%' AND query_kind = 'Select'
ORDER BY log_comment;

SELECT
    log_comment,
    if(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] >= 1, 'OK',
       format('Fail: no switch to GraceHashJoin, query_id="{}"', query_id))
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_date >= yesterday()
    AND log_comment LIKE '05245_full_all_filtered%' AND query_kind = 'Select'
ORDER BY log_comment;

DROP TABLE er_events;
DROP TABLE er_payloads;
DROP TABLE er_flags;
DROP TABLE fa_left;
DROP TABLE fa_right;
