-- Tags: shard

-- Regression test for predicate pushdown through a VIEW that does UNION ALL over
-- Distributed tables (https://github.com/ClickHouse/ClickHouse/issues/91641).
-- The outer WHERE must reach the primary key of every underlying MergeTree branch
-- on the remote shard, otherwise the query degrades to a full scan of each shard.

DROP VIEW IF EXISTS v_pankou;
DROP TABLE IF EXISTS t_legacy;
DROP TABLE IF EXISTS t_v1;

CREATE TABLE t_v1
(
    provider LowCardinality(String),
    code LowCardinality(String),
    field LowCardinality(String),
    value String,
    data_ts UInt64,
    minus_ts Int64
)
ENGINE = ReplacingMergeTree(minus_ts)
PARTITION BY provider
ORDER BY (code, data_ts, field, value);

CREATE TABLE t_legacy
(
    code LowCardinality(String),
    field LowCardinality(String),
    value String,
    data_ts UInt64,
    minus_ts Int64
)
ENGINE = ReplacingMergeTree(minus_ts)
ORDER BY (code, data_ts, field, value);

-- A view over two Distributed branches joined by UNION ALL, mirroring the issue.
-- A single remote shard (127.0.0.2) is used so `EXPLAIN distributed = 1` always
-- exposes the remote-side subplan of each branch: the test must verify the
-- predicate reaches the primary key on the REMOTE shard, not just the initiator.
CREATE VIEW v_pankou AS
SELECT code, field, value, data_ts, minus_ts
FROM remote('127.0.0.2', currentDatabase(), t_legacy)
UNION ALL
SELECT code, field, value, data_ts, minus_ts
FROM remote('127.0.0.2', currentDatabase(), t_v1)
WHERE provider = 'p0';

INSERT INTO t_v1
SELECT 'p0', leftPad(toString(number), 6, '0'), 'f1', toString(number),
       toUInt64(toDateTime('2025-12-10')), -number
FROM numbers(16);

INSERT INTO t_legacy
SELECT leftPad(toString(number), 6, '0'), 'f1', toString(number),
       toUInt64(toDateTime('2025-12-10')), -number
FROM numbers(16);

-- Topology-independent assertion: in the distributed plan every underlying
-- MergeTree scan (one per UNION ALL branch, on the remote shard) must carry the
-- pushed `code` primary-key condition, and both branch tables must be present.
-- Asserting (conditions == scans) instead of a fixed count keeps it independent
-- of how many shards/replicas the runner expands; a regression that stopped
-- pushing the predicate to either branch would leave conditions < scans.
-- The plan-affecting settings are pinned because the stateless runner randomizes
-- them, and any of them turned off could make the result an artifact, not a signal.
-- serialize_query_plan is pinned to 0 (the "distributed plan" CI job turns it on
-- via a profile): with a serialized plan the initiator renders the remote side as
-- a ReadFromTable placeholder and the per-shard index analysis is not visible here,
-- so the assertion needs the non-serialized plan to observe the remote PK condition.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree (%t_legacy)%') > 0
    AND countIf(explain LIKE '%ReadFromMergeTree (%t_v1)%') > 0
    AND countIf(explain LIKE '%ReadFromRemote%') >= 2
    AND countIf(explain LIKE '%ReadFromMergeTree%') >= 2
    AND countIf(explain LIKE '%code in [''000001'', ''000001'']%')
        = countIf(explain LIKE '%ReadFromMergeTree%')
FROM
(
    EXPLAIN indexes = 1, distributed = 1
    SELECT * FROM v_pankou
    WHERE toDate(data_ts) >= '2025-12-05' AND code = '000001' AND field IN ('f1', 'f2')
)
SETTINGS
    enable_analyzer = 1,
    enable_optimize_predicate_expression = 1,
    query_plan_enable_optimizations = 1,
    query_plan_merge_filters = 1,
    optimize_move_to_prewhere = 1,
    query_plan_optimize_prewhere = 1,
    enable_parallel_replicas = 0,
    serialize_query_plan = 0;

DROP VIEW v_pankou;
DROP TABLE t_legacy;
DROP TABLE t_v1;
