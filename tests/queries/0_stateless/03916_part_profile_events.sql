-- Tags: no-fasttest

DROP TABLE IF EXISTS part_profile_events SYNC;

CREATE TABLE part_profile_events (key UInt32, value String) Engine=MergeTree ORDER BY key SETTINGS disk = 's3_disk';
INSERT INTO part_profile_events SELECT number, toString(number) FROM numbers(100);

SYSTEM FLUSH LOGS part_log;

SELECT count() FROM part_profile_events;
-- Pin the assertion to the NewPart row(s) written by THIS run's insert: among the NewPart events for
-- this table in this run's database, keep only those at the latest `event_time_microseconds`. A retry
-- within two minutes leaves an older (hence excluded) row, and the part removal logged by `DROP TABLE`
-- is a different `event_type`, so neither can contribute a stale, often-zero row that would hide a
-- missing `S3PutObject` in the current insert.
SELECT max(ProfileEvents['S3PutObject'] > 0)
FROM system.part_log
WHERE event_type = 'NewPart'
  AND table = 'part_profile_events'
  AND database = currentDatabase()
  AND event_time_microseconds =
  (
      SELECT max(event_time_microseconds)
      FROM system.part_log
      WHERE event_type = 'NewPart'
        AND table = 'part_profile_events'
        AND database = currentDatabase()
  );
