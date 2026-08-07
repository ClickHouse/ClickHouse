-- Tags: no-fasttest

DROP TABLE IF EXISTS part_profile_events SYNC;

CREATE TABLE part_profile_events (key UInt32, value String) Engine=MergeTree ORDER BY key SETTINGS disk = 's3_disk';
INSERT INTO part_profile_events SELECT number, toString(number) FROM numbers(100);

SYSTEM FLUSH LOGS part_log;

SELECT count() FROM part_profile_events;
-- Pin the assertion to THIS run's table incarnation: `DROP`/`CREATE` gives the table a fresh UUID,
-- so filtering by `table_uuid` excludes any `NewPart` row left by a previous attempt in the same
-- database, and `count() > 0` proves the current insert actually wrote a row instead of silently
-- passing on a stale one when it did not.
SELECT count() > 0 AND min(ProfileEvents['S3PutObject'] > 0)
FROM system.part_log
WHERE event_type = 'NewPart'
  AND table = 'part_profile_events'
  AND database = currentDatabase()
  AND table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'part_profile_events');
