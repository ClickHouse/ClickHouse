-- Tags: no-replicated-database

DROP TABLE IF EXISTS t_proj_merge;
DROP TABLE IF EXISTS t_proj_rebuild;

-- ============================================================================
-- Scenario 1: Merge — Projection Merge (Phase B)
-- All source parts have projections, so they are merged directly.
-- Uses plain MergeTree to test MergePlainMergeTreeTask path.
-- ============================================================================
CREATE TABLE t_proj_merge
(
    key UInt64,
    value String,
    PROJECTION p1 (SELECT key, value ORDER BY value),
    PROJECTION p2 (SELECT key, count() GROUP BY key)
)
ENGINE = MergeTree()
ORDER BY key;

INSERT INTO t_proj_merge SELECT number, toString(number) FROM numbers(5000);
INSERT INTO t_proj_merge SELECT number + 5000, toString(number + 5000) FROM numbers(5000);

OPTIMIZE TABLE t_proj_merge FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT
    'scenario 1: merge projection merge',
    event_type,
    mapContains(projections_duration_ms, 'p1') AS has_p1,
    mapContains(projections_duration_ms, 'p2') AS has_p2
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 't_proj_merge'
    AND event_type = 'MergeParts'
ORDER BY event_time_microseconds
LIMIT 1;

-- NewPart (INSERT) should have an empty map
SELECT
    'scenario 1: insert has empty map',
    projections_duration_ms
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 't_proj_merge'
    AND event_type = 'NewPart'
ORDER BY event_time_microseconds
LIMIT 1;

-- ============================================================================
-- Scenario 2: Merge — Projection Rebuild (Phase A)
-- ReplacingMergeTree merges may reduce rows, so projections are rebuilt.
-- ============================================================================
CREATE TABLE t_proj_rebuild
(
    key UInt64,
    value String,
    PROJECTION p1 (SELECT key, value ORDER BY value)
)
ENGINE = ReplacingMergeTree()
ORDER BY key
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_proj_rebuild SELECT number, toString(number) FROM numbers(5000);
INSERT INTO t_proj_rebuild SELECT number, toString(number + 100000) FROM numbers(5000);

OPTIMIZE TABLE t_proj_rebuild FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT
    'scenario 2: merge projection rebuild',
    event_type,
    mapContains(projections_duration_ms, 'p1') AS has_p1
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 't_proj_rebuild'
    AND event_type = 'MergeParts'
ORDER BY event_time_microseconds
LIMIT 1;

-- ============================================================================
-- Scenario 3: ReplicatedMergeTree — Projection Merge (Phase B)
-- Tests the MergeFromLogEntryTask path (replicated merge glue code).
-- ============================================================================
DROP TABLE IF EXISTS t_proj_repl;

SET insert_keeper_fault_injection_probability = 0;

CREATE TABLE t_proj_repl
(
    key UInt64,
    value String,
    PROJECTION p1 (SELECT key, value ORDER BY value),
    PROJECTION p2 (SELECT key, count() GROUP BY key)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04004_t_proj_repl', 'r1')
ORDER BY key;

INSERT INTO t_proj_repl SELECT number, toString(number) FROM numbers(5000);
INSERT INTO t_proj_repl SELECT number + 5000, toString(number + 5000) FROM numbers(5000);

OPTIMIZE TABLE t_proj_repl FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT
    'scenario 3: replicated projection merge',
    event_type,
    mapContains(projections_duration_ms, 'p1') AS has_p1,
    mapContains(projections_duration_ms, 'p2') AS has_p2
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 't_proj_repl'
    AND event_type = 'MergeParts'
ORDER BY event_time_microseconds
LIMIT 1;

DROP TABLE t_proj_merge;
DROP TABLE t_proj_rebuild;
DROP TABLE t_proj_repl;
