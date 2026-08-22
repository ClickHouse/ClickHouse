-- Tags: no-async-insert
-- no-async-insert: 1 part is inserted with async inserts
DROP TABLE IF EXISTS t_parts_profile_events;

CREATE TABLE t_parts_profile_events (a UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 10, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_parts_profile_events;

SET log_comment = '02306_part_types_profile_events';

INSERT INTO t_parts_profile_events VALUES (1);
INSERT INTO t_parts_profile_events VALUES (1);

SYSTEM START MERGES t_parts_profile_events;
OPTIMIZE TABLE t_parts_profile_events FINAL;
SYSTEM STOP MERGES t_parts_profile_events;

INSERT INTO t_parts_profile_events SELECT number FROM numbers(20);

SYSTEM START MERGES t_parts_profile_events;
OPTIMIZE TABLE t_parts_profile_events FINAL;
SYSTEM STOP MERGES t_parts_profile_events;

SYSTEM FLUSH LOGS query_log, part_log;

SELECT count(), sum(ProfileEvents['InsertedWideParts']), sum(ProfileEvents['InsertedCompactParts'])
    FROM system.query_log WHERE current_database = currentDatabase()
        AND log_comment = '02306_part_types_profile_events'
        AND query ILIKE 'INSERT INTO%' AND type = 'QueryFinish';

SELECT count(), sum(ProfileEvents['MergedIntoWideParts']), sum(ProfileEvents['MergedIntoCompactParts'])
    FROM system.query_log WHERE current_database = currentDatabase()
        AND log_comment = '02306_part_types_profile_events'
        AND query ILIKE 'OPTIMIZE TABLE%' AND type = 'QueryFinish';

SELECT part_type FROM system.part_log WHERE database = currentDatabase()
    AND table = 't_parts_profile_events' AND event_type = 'NewPart'
    ORDER BY event_time_microseconds;

SELECT part_type, count() > 0 FROM system.part_log WHERE database = currentDatabase()
    AND table = 't_parts_profile_events' AND event_type = 'MergeParts'
    GROUP BY part_type ORDER BY part_type;

DROP TABLE t_parts_profile_events;
