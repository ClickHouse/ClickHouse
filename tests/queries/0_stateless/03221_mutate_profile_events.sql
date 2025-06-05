-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_mutate_profile_events;

CREATE TABLE t_mutate_profile_events (key UInt64, id UInt64, v1 UInt64, v2 UInt64)
ENGINE = MergeTree ORDER BY id PARTITION BY key
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_mutate_profile_events SELECT 1, number, number, number FROM numbers(10000);
INSERT INTO t_mutate_profile_events SELECT 2, number, number, number FROM numbers(10000);

SET mutations_sync = 2;

ALTER TABLE t_mutate_profile_events UPDATE v1 = 1000 WHERE key = 1;
ALTER TABLE t_mutate_profile_events DELETE WHERE key = 2 AND v2 % 10 = 0;

SYSTEM FLUSH LOGS;

SELECT
    splitByChar('_', part_name)[-1] AS version,
    sum(ProfileEvents['MutationTotalParts']),
    sum(ProfileEvents['MutationUntouchedParts']),
    sum(ProfileEvents['MutatedRows']),
    sum(ProfileEvents['MutatedUncompressedBytes']),
    sum(ProfileEvents['MutationAllPartColumns']),
    sum(ProfileEvents['MutationSomePartColumns']),
    sum(ProfileEvents['MutationTotalMilliseconds']) > 0,
    sum(ProfileEvents['MutationExecuteMilliseconds']) > 0,
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_mutate_profile_events' AND event_type = 'MutatePart'
GROUP BY version ORDER BY version;

DROP TABLE IF EXISTS t_mutate_profile_events
