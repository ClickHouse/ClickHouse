-- Tags: no-parallel-replicas

-- Hash-key equality differs from the merge comparison for floating-point join keys: `+0.0` and
-- `-0.0` (and NaNs with different payloads) hash as distinct keys but compare equal, so the
-- order-preserving MergeJoin spill fallback must be rejected for such keys (nested ones
-- included) - otherwise crossing the spill threshold would change the result.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 250000, max_bytes_ratio_before_external_join = 0;
-- Keep the ORDER BY of the parity subqueries, so that the in-order pin is really attempted.
SET query_plan_remove_redundant_sorting = 0;

DROP TABLE IF EXISTS fl_events;
DROP TABLE IF EXISTS fl_payloads;

CREATE TABLE fl_events (t UInt32, id Float64) ENGINE = MergeTree ORDER BY t;
INSERT INTO fl_events VALUES (0, 0.), (1, -0.), (2, nan), (3, -nan), (4, 1.5);

CREATE TABLE fl_payloads (id Float64, payload String) ENGINE = MergeTree ORDER BY tuple();
-- The last NaN carries another payload.
INSERT INTO fl_payloads VALUES (0., 'zero'), (-0., 'neg zero'), (nan, 'nan'), (reinterpretAsFloat64(toUInt64(9221120237041090561)), 'nan payload'), (1.5, 'p');
-- Push the right side past the spill threshold with unmatched keys.
INSERT INTO fl_payloads SELECT 1000000 + number, concat('p', toString(number)) FROM numbers(50000);

SELECT '-- Float keys must not read in order through the spilling join';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT fl_events.t, fl_payloads.payload FROM fl_events
    INNER JOIN fl_payloads ON fl_events.id = fl_payloads.id
    ORDER BY fl_events.t LIMIT 3
    SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%ReadType%';

SELECT '-- The spilled result keeps the hash-key equality (signed zeros, NaN payloads)';

-- Bitwise matching: 4 rows (each zero pairs only with its own sign, `nan` only with the
-- same-bits 'nan' row, `-nan` with nothing). The merge comparison would produce 9.
SELECT count() FROM (
    SELECT fl_events.t AS t FROM fl_events
    INNER JOIN fl_payloads ON fl_events.id = fl_payloads.id
    ORDER BY fl_events.t
    SETTINGS join_algorithm = 'hash'
);

SELECT
    (SELECT sum(cityHash64(t, payload)) FROM (
        SELECT fl_events.t AS t, fl_payloads.payload AS payload FROM fl_events
        INNER JOIN fl_payloads ON fl_events.id = fl_payloads.id
        ORDER BY fl_events.t
        SETTINGS join_algorithm = 'hash'
    ))
    =
    (SELECT sum(cityHash64(t, payload)) FROM (
        SELECT fl_events.t AS t, fl_payloads.payload AS payload FROM fl_events
        INNER JOIN fl_payloads ON fl_events.id = fl_payloads.id
        ORDER BY fl_events.t
        SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0
    ));

SELECT '-- Nested float keys are rejected too';

DROP TABLE IF EXISTS tn_events;
DROP TABLE IF EXISTS tn_payloads;
CREATE TABLE tn_events (t UInt32, key Tuple(UInt32, Float64)) ENGINE = MergeTree ORDER BY t;
INSERT INTO tn_events VALUES (0, (1, 0.)), (1, (1, -0.));
CREATE TABLE tn_payloads (key Tuple(UInt32, Float64), payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tn_payloads VALUES ((1, 0.), 'zero'), ((1, -0.), 'neg zero');

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT tn_events.t, tn_payloads.payload FROM tn_events
    INNER JOIN tn_payloads ON tn_events.key = tn_payloads.key
    ORDER BY tn_events.t LIMIT 3
    SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%ReadType%';

DROP TABLE fl_events;
DROP TABLE fl_payloads;
DROP TABLE tn_events;
DROP TABLE tn_payloads;
