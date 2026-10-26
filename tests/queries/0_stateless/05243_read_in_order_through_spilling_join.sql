-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- Read-in-order through a spilling hash join: when the query plan needs the left stream's row
-- order, `SpillingHashJoin` is pinned to spill through `MergeJoin` (which spills only the sorted
-- right side and streams every left block through once, in order) instead of `GraceHashJoin`
-- (which scatters rows into buckets by hash). The pin makes the read-in-order-through-join
-- optimization applicable even when the join spills to disk.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET query_plan_join_swap_table = 0;
SET join_use_nulls = 0;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

-- Force the switch to the external join during the build phase: a small absolute threshold, with
-- the ratio-based one disabled for determinism. The threshold also sizes the probe-time cache of
-- the sorted right blocks, so it is not too tiny.
SET max_bytes_before_external_join = 250000, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS payloads;

-- The granularity is pinned: the in-order LIMIT read below cuts at least one granule-sized
-- task per stream, and a CI-randomized granularity could blow through the read_rows bound.
CREATE TABLE events (t UInt32, id UInt32) ENGINE = MergeTree ORDER BY t
    SETTINGS index_granularity = 2048;
-- `id` swaps every adjacent pair of numbers, so the probe blocks are NOT sorted by the join key
-- and the order-preserving probe has to work through a real permutation, while the matches stay
-- local to keep the disk cache of the sorted right blocks effective.
INSERT INTO events SELECT number, bitXor(number, 1) FROM numbers(100000);

CREATE TABLE payloads (id UInt32, payload String) ENGINE = MergeTree ORDER BY tuple();
-- Every id of [0, 25000) exactly twice: ALL joins get a 2x fanout, ANY joins have to pick one
-- match, and left ids of [25000, 100000) stay unmatched.
INSERT INTO payloads SELECT number % 25000, concat('p', toString(number)) FROM numbers(50000);

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

SELECT '-- The left read must be in order (LEFT and INNER, hash and parallel_hash)';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT events.t, payloads.payload FROM events
    LEFT JOIN payloads ON events.id = payloads.id
    ORDER BY events.t LIMIT 3
    SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%ReadType%';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT events.t, payloads.payload FROM events
    INNER JOIN payloads ON events.id = payloads.id
    ORDER BY events.t LIMIT 3
    SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%ReadType%';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT events.t, payloads.payload FROM events
    LEFT JOIN payloads ON events.id = payloads.id
    ORDER BY events.t LIMIT 3
    SETTINGS join_algorithm = 'parallel_hash'
) WHERE explain LIKE '%ReadType%';

SELECT '-- LIMIT over the spilled join reads only a prefix of the left table';

-- `payload` stays out of the output: the order of the two rows of one `t` is not deterministic.
SELECT events.t, events.id FROM events
INNER JOIN payloads ON events.id = payloads.id
ORDER BY events.t LIMIT 3
SETTINGS join_algorithm = 'hash', log_comment = '05243_read_in_order_through_spilling_join',
         max_block_size = DEFAULT;

SELECT '-- Full results: correct multisets, emitted in the left order';

-- INNER ALL: 25000 matched left rows, fanout 2.
SELECT count(), uniqExact(t) FROM (
    SELECT events.t AS t FROM events INNER JOIN payloads ON events.id = payloads.id
    SETTINGS join_algorithm = 'hash'
);

-- The stream keeps the left order even without a Sorting step on top of the join.
SELECT arraySort(g) = g FROM (
    SELECT groupArray(materialize(t)) AS g FROM (
        SELECT events.t AS t FROM events INNER JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash', max_threads = 1
    )
);

-- LEFT ALL: 50000 matched rows + 75000 unmatched with a default payload.
SELECT count(), countIf(payload = ''), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT events.t AS t, payloads.payload AS payload FROM events
    LEFT JOIN payloads ON events.id = payloads.id
    ORDER BY events.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

-- LEFT ANY: exactly one output row per left row, in the left order.
SELECT count(), countIf(payload = ''), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT events.t AS t, payloads.payload AS payload FROM events
    LEFT ANY JOIN payloads ON events.id = payloads.id
    ORDER BY events.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

-- INNER ANY: one output row per matched key, in the left order (the left keys are unique, so
-- exactly one deterministic left row per key).
SELECT count(), uniqExact(t), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT events.t AS t FROM events
    INNER ANY JOIN payloads ON events.id = payloads.id
    ORDER BY events.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

SELECT '-- Concurrent build: parallel_hash switches to MergeJoin from multiple threads';

SELECT count(), uniqExact(t), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT events.t AS t FROM events INNER JOIN payloads ON events.id = payloads.id
    ORDER BY events.t
    SETTINGS join_algorithm = 'parallel_hash', max_threads = 8
);

SELECT '-- The spilled results match the non-spilled hash join (INNER/LEFT x ALL/ANY)';

-- Every kind/strictness `canPreserveLeftBlockOrder` accepts: the ORDER BY inside the subqueries
-- pins the order-preserving MergeJoin fallback on the spilled side (the session threshold is the
-- "tiny" one, 250000), while `max_bytes_before_external_join = 0` keeps the other side on the
-- in-memory hash join. The ANY checksums exclude `payload`: every key has two duplicate right
-- rows and ANY picks one of them nondeterministically (and differently across the algorithms);
-- the left columns and `payload != ''` (matched or not) are deterministic because the left keys
-- are unique.

-- The checksum aggregation is order-insensitive, so the redundant-sorting removal would drop the
-- inner ORDER BY - and with it the MergeJoin pin - before the read-in-order gate sees it.
SET query_plan_remove_redundant_sorting = 0;

SELECT
    (SELECT sum(cityHash64(t, id, payload)) FROM (
        SELECT events.t AS t, events.id AS id, payloads.payload AS payload FROM events
        INNER JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash'
    ))
    =
    (SELECT sum(cityHash64(t, id, payload)) FROM (
        SELECT events.t AS t, events.id AS id, payloads.payload AS payload FROM events
        INNER JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ));

SELECT
    (SELECT sum(cityHash64(t, id, payload)) FROM (
        SELECT events.t AS t, events.id AS id, payloads.payload AS payload FROM events
        LEFT JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash'
    ))
    =
    (SELECT sum(cityHash64(t, id, payload)) FROM (
        SELECT events.t AS t, events.id AS id, payloads.payload AS payload FROM events
        LEFT JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ));

SELECT
    (SELECT sum(cityHash64(t, id)) FROM (
        SELECT events.t AS t, events.id AS id FROM events
        INNER ANY JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash'
    ))
    =
    (SELECT sum(cityHash64(t, id)) FROM (
        SELECT events.t AS t, events.id AS id FROM events
        INNER ANY JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ));

SELECT
    (SELECT sum(cityHash64(t, id, payload != '')) FROM (
        SELECT events.t AS t, events.id AS id, payloads.payload AS payload FROM events
        LEFT ANY JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash'
    ))
    =
    (SELECT sum(cityHash64(t, id, payload != '')) FROM (
        SELECT events.t AS t, events.id AS id, payloads.payload AS payload FROM events
        LEFT ANY JOIN payloads ON events.id = payloads.id
        ORDER BY events.t
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ));

SET query_plan_remove_redundant_sorting = DEFAULT;

SELECT '-- DISTINCT in order over the spilled join';

SELECT count() FROM (
    SELECT DISTINCT events.t FROM events INNER JOIN payloads ON events.id = payloads.id
    SETTINGS join_algorithm = 'hash', optimize_distinct_in_order = 1
);

SELECT '-- The join did spill through MergeJoin, and the read was short';

SYSTEM FLUSH LOGS system.query_log;

-- The right table (50000 rows) is always read fully for the build; a full left scan would push
-- the total to 150000 rows, the in-order read keeps it well below.
SELECT
    if(read_rows < 100_000, 'OK', format('Fail: {} rows read, query_id="{}"', read_rows, query_id)),
    if(ProfileEvents['JoinSpillingHashJoinSwitchedToMergeJoin'] >= 1, 'OK',
       format('Fail: no switch to MergeJoin, query_id="{}"', query_id)),
    if(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] = 0, 'OK',
       format('Fail: switched to GraceHashJoin, query_id="{}"', query_id))
FROM system.query_log
WHERE type = 'QueryFinish'
    AND log_comment = '05243_read_in_order_through_spilling_join'
    AND current_database = currentDatabase()
    AND event_time >= (SELECT ts FROM start_ts)
    AND event_time >= yesterday()
    AND query_kind = 'Select';

SELECT '-- Left order uncorrelated with the join key: matches hop between the spilled right blocks';

-- The matched right rows are gathered at match time, so the emission does not reload the right
-- blocks even though consecutive left rows reference distant ones.
DROP TABLE IF EXISTS events_scattered;
CREATE TABLE events_scattered (t UInt32, id UInt32) ENGINE = MergeTree ORDER BY t;
-- `id` is a permutation of [0, 100000): 7919 is coprime with 100000.
INSERT INTO events_scattered SELECT number, (number * 7919) % 100000 FROM numbers(100000);

SELECT count(), uniqExact(t), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT events_scattered.t AS t FROM events_scattered
    INNER JOIN payloads ON events_scattered.id = payloads.id
    ORDER BY events_scattered.t
    SETTINGS join_algorithm = 'hash', max_threads = 1
);

SELECT
    (SELECT sum(cityHash64(t, id, payload)) FROM (
        SELECT events_scattered.t AS t, events_scattered.id AS id, payloads.payload AS payload FROM events_scattered
        LEFT JOIN payloads ON events_scattered.id = payloads.id
        SETTINGS join_algorithm = 'hash'
    ))
    =
    (SELECT sum(cityHash64(t, id, payload)) FROM (
        SELECT events_scattered.t AS t, events_scattered.id AS id, payloads.payload AS payload FROM events_scattered
        LEFT JOIN payloads ON events_scattered.id = payloads.id
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ));

DROP TABLE events_scattered;
DROP TABLE events;
DROP TABLE payloads;
