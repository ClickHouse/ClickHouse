-- A MATERIALIZED column reading an EPHEMERAL one is never recomputed by a mutation, so such a Row
-- cannot stand in for its source columns and must not be treated as a wrapper.

DROP TABLE IF EXISTS row_ephemeral_source;

SET allow_experimental_row_type = 1;
CREATE TABLE row_ephemeral_source
(
    id UInt64,
    e UInt64 EPHEMERAL,
    a UInt64,
    b UInt64,
    w Row(e UInt64, a UInt64, b UInt64) MATERIALIZED tuple(e, a, b)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO row_ephemeral_source (id, e, a, b) VALUES (1, 9, 10, 30);

-- The mutation warns that `w` will not be recalculated; that warning is the point of this test.
SET send_logs_level = 'error';
ALTER TABLE row_ephemeral_source UPDATE a = 20 WHERE id = 1 SETTINGS mutations_sync = 2;

-- The wrapper still holds the pre-mutation value, so the rewrite must not fire.
SELECT w FROM row_ephemeral_source;
SELECT a, b FROM row_ephemeral_source SETTINGS query_plan_use_row_wrappers = 1;
SELECT a, b FROM row_ephemeral_source SETTINGS query_plan_use_row_wrappers = 0;

SELECT countIf(explain LIKE '%__rowElement%') FROM (
    EXPLAIN actions = 1 SELECT a, b FROM row_ephemeral_source SETTINGS query_plan_use_row_wrappers = 1
);

DROP TABLE row_ephemeral_source;
