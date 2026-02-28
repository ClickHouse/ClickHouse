-- Tags: no-replicated-database, no-parallel

DROP TABLE IF EXISTS data_with_projection;
DROP TABLE IF EXISTS data_with_projection_rmt SYNC;

-- Test MergePlainMergeTreeTask path
CREATE TABLE data_with_projection
(
    key UInt64,
    value String,
    PROJECTION p1 (SELECT key, value ORDER BY value),
    PROJECTION p2 (SELECT key, count() GROUP BY key)
)
ENGINE = MergeTree()
ORDER BY key;

INSERT INTO data_with_projection SELECT number, toString(number) FROM numbers(50000);
INSERT INTO data_with_projection SELECT number + 50000, toString(number + 50000) FROM numbers(50000);

OPTIMIZE TABLE data_with_projection FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT
    event_type,
    mapContains(projections_duration_ms, 'p1') AS has_p1,
    mapContains(projections_duration_ms, 'p2') AS has_p2,
    projections_duration_ms['p1'] > 0 AS p1_duration_positive,
    projections_duration_ms['p2'] > 0 AS p2_duration_positive
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 'data_with_projection'
    AND event_type = 'MergeParts'
ORDER BY event_time_microseconds
LIMIT 1;

SELECT
    event_type,
    projections_duration_ms
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 'data_with_projection'
    AND event_type = 'NewPart'
ORDER BY event_time_microseconds
LIMIT 1;

-- Test MergeFromLogEntryTask path (ReplicatedMergeTree)
CREATE TABLE data_with_projection_rmt
(
    key UInt64,
    value String,
    PROJECTION p1 (SELECT key, value ORDER BY value),
    PROJECTION p2 (SELECT key, count() GROUP BY key)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04004/data_with_projection_rmt', 'r1')
ORDER BY key;

SYSTEM STOP MERGES data_with_projection_rmt;

INSERT INTO data_with_projection_rmt SELECT number, toString(number) FROM numbers(50000);
INSERT INTO data_with_projection_rmt SELECT number + 50000, toString(number + 50000) FROM numbers(50000);

SYSTEM START MERGES data_with_projection_rmt;
OPTIMIZE TABLE data_with_projection_rmt FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT
    event_type,
    mapContains(projections_duration_ms, 'p1') AS has_p1,
    mapContains(projections_duration_ms, 'p2') AS has_p2,
    projections_duration_ms['p1'] > 0 AS p1_duration_positive,
    projections_duration_ms['p2'] > 0 AS p2_duration_positive
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 'data_with_projection_rmt'
    AND event_type = 'MergeParts'
ORDER BY event_time_microseconds
LIMIT 1;

SELECT
    event_type,
    projections_duration_ms
FROM system.part_log
WHERE
    event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 'data_with_projection_rmt'
    AND event_type = 'NewPart'
ORDER BY event_time_microseconds
LIMIT 1;

DROP TABLE data_with_projection;
DROP TABLE data_with_projection_rmt SYNC;
